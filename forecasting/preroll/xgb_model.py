import datetime
import os
import sys
import time
import pickle
from functools import reduce
from math import log
import itertools
import math
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.shell import spark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import pandas as pd

from sklearn.datasets import load_boston
from sklearn.model_selection import train_test_split
from sklearn.inspection import permutation_importance
from sklearn import metrics, linear_model
from sklearn.ensemble import RandomForestRegressor

from xgboost import XGBRegressor, XGBClassifier
# from xgboost import plot_tree
# import matplotlib.pyplot as plt
# pip3 install xgboost==1.6.2
# pip3 install matplotlib
# pip3 install numpy --upgrade
# pip3 install Graphviz
# sudo yum install graphviz
storageLevel = StorageLevel.DISK_ONLY


def check_s3_path_exist(s3_path: str) -> bool:
    if not s3_path.endswith("/"):
        s3_path += "/"
    return os.system(f"aws s3 ls {s3_path}_SUCCESS") == 0


def get_date_list(date: str, days: int) -> list:
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    if -1 <= days <= 1:
        return [date]
    elif days > 1:
        return [(dt + datetime.timedelta(days=n)).strftime('%Y-%m-%d') for n in range(0, days)]
    else:
        return [(dt + datetime.timedelta(days=n)).strftime('%Y-%m-%d') for n in range(days + 1, 1)]


def load_data_frame(spark: SparkSession, path: str, fmt: str = 'parquet', header: bool = False, delimiter: str = ','
                    ) -> DataFrame:
    if fmt == 'parquet':
        return spark.read.parquet(path)
    elif fmt == 'orc':
        return spark.read.orc(path)
    elif fmt == 'jsond':
        return spark.read.json(path)
    elif fmt == 'csv':
        return spark.read.option('header', header).option('delimiter', delimiter).csv(path)
    else:
        print("the format is not supported")
        return DataFrame(None, None)


def hive_spark(name: str) -> SparkSession:
    return SparkSession.builder \
        .appName(name) \
        .config("hive.metastore.uris", "thrift://metastore.data.hotstar-labs.com:9083") \
        .config("spark.kryoserializer.buffer.max", "128m") \
        .enableHiveSupport() \
        .getOrCreate()


def load_hive_table(spark: SparkSession, table: str, date: str = None) -> DataFrame:
    if date is None:
        return spark.sql(f'select * from {table}')
    else:
        return spark.sql(f'select * from {table} where cd = "{date}"')


def save_data_frame(df: DataFrame, path: str, fmt: str = 'parquet', header: bool = False, delimiter: str = ',') -> None:
    def save_data_frame_internal(df: DataFrame, path: str, fmt: str = 'parquet', header: bool = False,
                                 delimiter: str = ',') -> None:
        if fmt == 'parquet':
            df.write.mode('overwrite').parquet(path)
        elif fmt == 'parquet2':
            df.write.mode('overwrite').parquet(path, compression='gzip')
        elif fmt == 'parquet3':
            df.write.mode('overwrite').parquet(path, compression='uncompressed')
        elif fmt == 'orc':
            df.write.mode('overwrite').orc(path)
        elif fmt == 'csv':
            df.coalesce(1).write.option('header', header).option('delimiter', delimiter).mode('overwrite').csv(path)
        elif fmt == 'csv_zip':
            df.write.option('header', header).option('delimiter', delimiter).option("compression", "gzip").mode(
                'overwrite').csv(path)
        else:
            print("the format is not supported")
    df.persist(storageLevel)
    try:
        save_data_frame_internal(df, path, fmt, header, delimiter)
    except Exception:
        try:
            save_data_frame_internal(df, path, 'parquet2', header, delimiter)
        except Exception:
            save_data_frame_internal(df, path, 'parquet3', header, delimiter)
    df.unpersist()


def load_labels(tournament):
    if tournament == "wc2022":
        data_source = "watched_video"
    else:
        data_source = "watched_video_sampled"
    df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/final_test_dataset/{data_source}_of_{tournament}")
    # date, content_id, title, shortsummary,
    # total_inventory, total_pid_reach, total_did_reach
    return df


def load_dataset(tournaments, feature_df, sorting=False, repeat_num_col="", mask_cols=[], mask_condition="", mask_rate=1, test_tournament="", sub_or_free=""):
    tournaments_str = ",".join([f'"{tournament}"' for tournament in tournaments])
    # print(tournaments_str)
    if test_tournament == "wc2019":
        if sorting:
            sample_tag = configuration['wc2019_test_tag']
        else:
            sample_tag = 3 - configuration['wc2019_test_tag']
            # sample_tag = "1, 2"
    else:
        sample_tag = "1, 2"
    new_feature_df = feature_df \
        .where(f'date != "2022-08-24" and tournament in ({tournaments_str}) and sample_tag in (0, {sample_tag})') \
        .cache()
    # new_feature_df = feature_df \
    #     .where(f'date != "2022-08-24" and tournament in ({tournaments_str})') \
    #     .cache()
    if repeat_num_col != "":
        new_feature_df = new_feature_df\
            .withColumn(repeat_num_col, n_to_array(repeat_num_col))\
            .withColumn(repeat_num_col, F.explode(F.col(repeat_num_col)))
    if mask_cols and mask_rate > 0:
        new_feature_df = new_feature_df \
            .withColumn('rank_tmp', F.expr('row_number() over (partition by content_id order by date)'))
        for mask_col in mask_cols:
            if mask_col.find('cross') > -1:
                new_feature_df = new_feature_df\
                    .withColumn(mask_col, F.expr(f'if({mask_condition} and rank_tmp <= {mask_rate}, empty_{mask_col}, {mask_col})'))
            else:
                new_feature_df = new_feature_df \
                    .withColumn("empty_" + mask_col, mask_array(mask_col)) \
                    .withColumn(mask_col, F.expr(f'if({mask_condition} and rank_tmp <= {mask_rate}, empty_{mask_col}, {mask_col})'))
        # if test_tournament == "wc2019":
        #     if sorting:
        #         new_feature_df = new_feature_df \
        #             .where(f'sample_tag in (0, {sample_tag})') \
        #             .cache()
        #     else:
        #         new_feature_df = new_feature_df \
        #             .where(f'sample_tag in (0, {sample_tag}) or (sample_tag in ({3 - sample_tag}) and {mask_condition} and rank_tmp > {mask_rate})') \
        #             .cache()
        #     new_feature_df.orderBy('date', 'content_id').where('tournament="wc2019"').select('date', 'match_stage', 'sample_tag', 'if_contain_india_team_hot_vector').show(200, False)
    if sorting:
        df = new_feature_df\
            .orderBy('date', 'content_id') \
            .toPandas()
    else:
        df = new_feature_df \
            .toPandas()
    return df


def enumerate_tiers(tiers_list):
    if 0 in tiers_list:
        return [tiers_list[0] + tiers_list[1]]
    else:
        return [tiers_list[0] + tiers_list[1] + 1]


def cross_features(dim, dim_list, *args):
    res = [0 for i in range(dim)]
    partition_dim = dim
    dim_idx = 0
    idx = 0
    for x in args:
        partition_dim /= dim_list[dim_idx]
        dim_idx += 1
        idx += x[0] * partition_dim
    res[int(idx)] = 1
    # print(idx)
    return res


def empty_cross_features(dim, dim_list, *args):
    res = [0 for i in range(dim)]
    partition_dim = dim
    dim_idx = 0
    idx = 0
    for x in args:
        partition_dim /= dim_list[dim_idx]
        if dim_idx == 0:
            idx += (dim_list[0] - 1) * partition_dim
        else:
            idx += x[0] * partition_dim
        dim_idx += 1
    res[int(idx)] = 1
    return res


def generate_hot_vector(hots, hots_num):
    res = [0 for i in range(hots_num)]
    for hot in hots:
        res[hot] += 1
    return res


def feature_processing(feature_df):
    match_rank_df = feature_df\
        .select('content_id', 'tournament', 'match_stage', 'rand')\
        .distinct()\
        .withColumn("match_rank", F.expr('row_number() over (partition by tournament, match_stage order by rand)'))\
        .select('content_id', 'match_rank')\
        .cache()
    return feature_df\
        .withColumn(repeat_num_col, F.expr(f'if({mask_condition}, {knock_off_repeat_num}, 1)'))\
        .withColumn("hostar_influence_hots_num", F.lit(1))\
        .withColumn("if_contain_india_team_hots_num", F.lit(if_contain_india_team_dim))\
        .withColumn("if_contain_india_team_hots", if_contain_india_team_hot_vector_udf('if_contain_india_team_hots', 'tournament_type'))\
        .withColumn("if_contain_india_team_hot_vector", generate_hot_vector_udf('if_contain_india_team_hots', 'if_contain_india_team_hots_num'))\
        .join(match_rank_df, 'content_id')\
        .withColumn("knock_rank", F.expr('row_number() over (partition by tournament order by date desc)'))\
        .withColumn("knock_rank", F.expr('if(match_stage not in ("final", "semi-final"), 100, knock_rank)'))\
        .withColumn("knock_rank_hot_vector", F.array(F.col('knock_rank'))) \
        .withColumn("knock_rank_hots_num", F.lit(1))\
        .withColumn("sample_tag", F.expr('if(tournament="wc2019", if(match_stage="group", if(rand<0.5, 1, 2), if(match_rank<=1, 1, 2)), 0)'))\
        .withColumn("sub_tag", F.array(F.lit(1)))\
        .withColumn("free_tag", F.array(F.lit(0)))\
        .withColumn("sub_free_tag_hots_num", F.lit(1))\
        .withColumn('sample_repeat_num', F.expr('if(content_id="1440000724", 2, 1)'))\
        .withColumn("match_num_hots_num", F.lit(1))


n_to_array = F.udf(lambda n: [n] * n, ArrayType(IntegerType()))
mask_array = F.udf(lambda l: [0 for i in l], ArrayType(IntegerType()))
enumerate_tiers_udf = F.udf(enumerate_tiers, ArrayType(IntegerType()))
order_match_stage_udf = F.udf(lambda x: [match_stage_dic[x]], ArrayType(IntegerType()))
if_contain_india_team_hot_vector_udf = F.udf(lambda x, y: x if y != "national" else [2], ArrayType(IntegerType()))
cross_features_udf = F.udf(cross_features, ArrayType(IntegerType()))
empty_cross_features_udf = F.udf(empty_cross_features, ArrayType(IntegerType()))
generate_hot_vector_udf = F.udf(generate_hot_vector, ArrayType(IntegerType()))

concurrency_root_path = "s3://hotstar-dp-datalake-processed-us-east-1-prod/hive_internal_database/concurrency.db/"
ssai_concurrency_path = f"{concurrency_root_path}/users_by_live_sports_content_by_ssai"
match_meta_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/ads_crash/match_meta"
live_ads_inventory_forecasting_root_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting"
play_out_log_input_path = "s3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test"
watchAggregatedInputPath = "s3://hotstar-dp-datalake-processed-us-east-1-prod/aggregates/watched_video_daily_aggregates_ist_v4"
viewAggregatedInputPath = "s3://hotstar-dp-datalake-processed-us-east-1-prod/aggregates/viewed_page_daily_aggregates_ist_v2"
live_ads_inventory_forecasting_complete_feature_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/complete_features"
preroll_live_ads_inventory_forecasting_root_path = f"{live_ads_inventory_forecasting_root_path}/preroll"

# spark.stop()
# spark = hive_spark('statistics')
# match_df = load_hive_table(spark, "in_cms.match_update_s3")
# save_data_frame(match_df, match_meta_path)

configuration = {
    'model': "xgb",
    # 'model': "random_forest",
    'tournament_list': "all",
    # 'tournament_list': "svod",
    # 'tournament_list': "t20",
    # 'task': "rate_prediction",
    # "task": 'test_prediction',
    'task': 'prediction_result_save',
    # 'mask_tag': "mask_knock_off",
    'mask_tag': "",
    'sample_weight': False,
    # 'sample_weight': True,
    'test_tournaments': ["ipl2022", "wc2022"],
    'predict_tournaments': ["ac2023", "wc2023"],
    'wc2019_test_tag': 1,
    # 'wc2019_test_tag': 2,
    # 'if_contains_language_and_platform': True,
    'if_contains_language_and_platform': False,
    'if_improve_ties': True,
    # 'if_improve_ties': False,
    'if_simple_one_hot': "_and_simple_one_hot",
    # 'if_simple_one_hot': "",
    # 'if_free_timer': "_and_free_timer",
    'if_free_timer': "",
    # 'if_hotstar_influence': False,
    'if_hotstar_influence': True,
    # 'if_teams': False,
    'if_teams': True,
    'if_cross_features': True,
    # 'if_cross_features': False,
    # 'cross_features': [['if_contain_india_team_hots', 'match_stage_hots', 'tournament_type_hots'],
    #                    ['if_contain_india_team_hots', 'match_type_hots', 'tournament_type_hots']],
    'cross_features': [['if_contain_india_team_hots', 'match_stage_hots', 'tournament_type_hots'],
                       ['if_contain_india_team_hots', 'match_type_hots', 'tournament_type_hots'],
                       ['if_contain_india_team_hots', 'vod_type_hots', 'tournament_type_hots']],
    # 'cross_features': [['if_contain_india_team_hot_vector', 'match_stage_hots', 'tournament_type_hots'],
    #                    ['if_contain_india_team_hot_vector', 'match_type_hots', 'tournament_type_hots'],
    #                    ['vod_type_hots', 'tournament_type_hots']],
    # 'cross_features': [['if_contain_india_team_hot_vector', 'match_stage_hots'],
    #                    ['if_contain_india_team_hot_vector', 'tournament_type_hots'],
    #                    ['match_stage_hots', 'tournament_type_hots']],
    # 'if_match_num': True,
    'if_match_num': False,
    # 'if_knock_off_rank': True,
    'if_knock_off_rank': False,
    'if_make_match_stage_ranked': False,
    # 'if_make_match_stage_ranked': True,
    # 'if_feature_weight': True,
    'if_feature_weight': False,
    'au_source': "avg_au",
    # 'au_source': "avg_predicted_au",
    'prediction_free_timer': 1000,
    # 'prediction_free_timer': 5,
    'end_tag': 0
}
match_stage_dic = {
    'qualifier': 0,
    'group': 1,
    'semi-final': 2,
    'final': 3
}

if_contain_india_team_dim = 3
hots_num_dic = {
    'if_contain_india_team_hots': if_contain_india_team_dim+1,
    'match_stage_hots': 4,
    'tournament_type_hots': 3,
    'match_type_hots': 3,
    'vod_type_hots': 2
}
label_df = load_data_frame(spark, f"{preroll_live_ads_inventory_forecasting_root_path}/inventory_data/gt_inventory_with_avg_session_num")
label_df.groupby('tournament').count().show(200, False)
tournament_list = [tournament[0] for tournament in label_df.select('tournament').distinct().collect()]
tournament_list.remove('wc2021')
print(tournament_list)

base_label_cols = ['free_avg_session_num', 'sub_avg_session_num']

if configuration['if_free_timer'] == "":
    label_cols = base_label_cols
    estimated_label_cols = [f'estimated_{label}' for label in label_cols]
else:
    label_cols = ["watch_time_per_free_per_match_with_free_timer"]
    estimated_label_cols = [f'estimated_{label}' for label in label_cols]


sub_pid_did_rate = 0.94
free_pid_did_rate = 1.02
prediction_vod_str = ""

repeat_union = 0.05
knock_off_repeat_num = 1
repeat_num_col = "knock_off_repeat_num"
mask_condition = 'match_stage in ("final", "semi-final")'
# path_suffix = "/all_features_hots_format_with_avg_au_sub_free_num" + configuration['if_free_timer'] + configuration['if_simple_one_hot']
path_suffix = f"/all_features_hots_format_with_{configuration['au_source']}_sub_free_num" + configuration['if_free_timer'] + configuration['if_simple_one_hot']
# match_num_df = load_data_frame(spark, live_ads_inventory_forecasting_complete_feature_path + "/match_num").cache()
feature_df = feature_processing(load_data_frame(spark, live_ads_inventory_forecasting_complete_feature_path + path_suffix))\
    .join(label_df.select('content_id', *base_label_cols), 'content_id')\
    .cache()

# feature_df.groupBy('tournament').count().show()
# .where('tournament != "wc2021" or (tournament = "wc2021" and if_contain_india_team = 1)')\

prediction_cols = load_data_frame(spark,
                                  live_ads_inventory_forecasting_complete_feature_path + "/" + configuration['predict_tournaments'][0] + "/all_features_hots_format" + configuration['if_simple_one_hot']).columns
predict_feature_df = feature_processing(reduce(lambda x, y: x.union(y), [load_data_frame(spark, live_ads_inventory_forecasting_complete_feature_path + "/" + tournament
                                              + "/all_features_hots_format" + configuration['if_simple_one_hot']).select(*prediction_cols) for tournament in configuration['predict_tournaments']])
    .withColumn("rand", F.rand(seed=54321)))\
    .join(label_df.select('content_id', *base_label_cols), 'content_id', 'left')\
    .fillna(1.0, base_label_cols)\
    .cache()

if len(configuration['cross_features']) == 3:
    prediction_vod_str += "_vod_cross"


if configuration['prediction_free_timer'] < 1000:
    predict_feature_df = predict_feature_df\
        .withColumn("vod_type_hots", F.array(F.lit(1)))\
        .withColumn("vod_type_hot_vector", F.array(F.lit(1)))\
        .cache()
    prediction_vod_str += "_svod"


print(prediction_vod_str)
# feature_df.where('knock_rank < 100').select('tournament', 'knock_rank', 'knock_rank_hot_vector', 'title').show(200, False)
# feature_df.where('tournament="wc2019"').select('date', 'title', 'rand', 'sample_tag').orderBy('date').show(200, False)
# feature_df.where('tournament="wc2019"').groupBy('match_stage', 'sample_tag', 'if_contain_india_team').count().orderBy('match_stage').show(200, False)
# print(feature_df.count())
# print(predict_feature_df.count())
# predict_feature_df.groupBy('tournament', 'vod_type').count().show()
# feature_df.groupBy('sample_tag').count().show()

one_hot_cols = ['tournament_type', 'if_weekend', 'match_time', 'if_holiday', 'venue', 'if_contain_india_team',
                'match_type', 'tournament_name', 'hostar_influence',
                'match_stage', 'vod_type', 'gender_type']
multi_hot_cols = ['teams', 'continents', 'teams_tier']
additional_cols = ["languages", "platforms"]
if not configuration['if_hotstar_influence']:
    one_hot_cols.remove('hostar_influence')

if not configuration['if_teams']:
    multi_hot_cols.remove('teams')

if configuration['if_knock_off_rank']:
    one_hot_cols = ['knock_rank'] + one_hot_cols

if configuration['if_match_num']:
    one_hot_cols = ['match_num'] + one_hot_cols

if configuration['if_improve_ties']:
    feature_df = feature_df \
        .withColumn("teams_tier_hot_vector", enumerate_tiers_udf('teams_tier_hots')) \
        .withColumn("teams_tier_hots_num", F.lit(1)) \
        .cache()
    predict_feature_df = predict_feature_df \
        .withColumn("teams_tier_hot_vector", enumerate_tiers_udf('teams_tier_hots')) \
        .withColumn("teams_tier_hots_num", F.lit(1)) \
        .cache()

mask_features = ['if_contain_india_team']
if configuration['if_cross_features']:
    for idx in range(len(configuration['cross_features'])):
        feature_dim_list = [hots_num_dic[feature] for feature in configuration['cross_features'][idx]]
        feature_dim = reduce(lambda x, y: x * y, feature_dim_list)
        print(feature_dim)
        feature_df = feature_df \
            .withColumn("cross_feature_dim_list", F.array(*map(F.lit, feature_dim_list))) \
            .withColumn(f"cross_features_{idx}_hots_num", F.lit(feature_dim)) \
            .withColumn(f"cross_features_{idx}_hot_vector", cross_features_udf(f"cross_features_{idx}_hots_num", "cross_feature_dim_list", *configuration['cross_features'][idx])) \
            .withColumn(f"empty_cross_features_{idx}_hot_vector", empty_cross_features_udf(f"cross_features_{idx}_hots_num", "cross_feature_dim_list", *(configuration['cross_features'][idx][1:]))) \
            .cache()
        predict_feature_df = predict_feature_df \
            .withColumn("cross_feature_dim_list", F.array(*map(F.lit, feature_dim_list))) \
            .withColumn(f"cross_features_{idx}_hots_num", F.lit(feature_dim)) \
            .withColumn(f"cross_features_{idx}_hot_vector", cross_features_udf(f"cross_features_{idx}_hots_num", "cross_feature_dim_list", *configuration['cross_features'][idx])) \
            .withColumn(f"empty_cross_features_{idx}_hot_vector", empty_cross_features_udf(f"cross_features_{idx}_hots_num", "cross_feature_dim_list", *(configuration['cross_features'][idx][1:]))) \
            .cache()
        one_hot_cols = [f'cross_features_{idx}'] + one_hot_cols
        mask_features.append(f'cross_features_{idx}')
        # feature_df.select(f"cross_features_{idx}_hots_num", f"cross_features_{idx}_hot_vector").show(20, False)

mask_cols = [col+"_hot_vector" for col in multi_hot_cols + mask_features]
feature_cols = [col+"_hot_vector" for col in one_hot_cols[:-1]+multi_hot_cols]
if configuration['if_contains_language_and_platform']:
    feature_cols += [col+"_hot_vector" for col in additional_cols]

if configuration['if_make_match_stage_ranked']:
    feature_df = feature_df \
        .withColumn("match_stage_hot_vector", order_match_stage_udf('match_stage')) \
        .withColumn("match_stage_hots_num", F.lit(1)) \
        .cache()
    predict_feature_df = predict_feature_df \
        .withColumn("match_stage_hot_vector", order_match_stage_udf('match_stage')) \
        .withColumn("match_stage_hots_num", F.lit(1)) \
        .cache()

if configuration['model'] == "xgb":
    tree_num_list = [n_estimators for n_estimators in range(1, 100, 4)]
    max_depth_list = [max_depth for max_depth in range(1, 15, 2)]
    learning_rate_list = [0.01, 0.05, 0.1, 0.2]
else:
    tree_num_list = [n_estimators for n_estimators in range(50, 201, 4)]
    max_depth_list = [max_depth for max_depth in range(10, 50, 2)]
    learning_rate_list = [0.01]

if configuration['if_feature_weight']:
    colsample_bynode = 0.8
else:
    colsample_bynode = 1.0

# print(colsample_bynode)
# one_hot_cols = ['if_weekend', 'match_time', 'if_holiday', 'venue', 'if_contain_india_team', 'match_type', 'tournament_name', 'hostar_influence', 'match_stage', 'gender_type']
# multi_hot_cols = ['teams', 'teams_tier']
# additional_cols = ["languages", "platforms"]
# feature_cols = [col+"_hot_vector" for col in one_hot_cols[:-1]+multi_hot_cols[:-1]+additional_cols]

# feature_cols = [col+"_hot_vector" for col in one_hot_cols[:-1]+multi_hot_cols[:-1]]
# feature_cols = [col+"_hot_vector" for col in ['teams', 'match_time', 'match_stage', 'if_holiday',
#                                               'if_weekend', 'hostar_influence', 'if_contain_india_team']]
feature_num_cols = [col.replace("_hot_vector", "_hots_num") for col in feature_cols]
feature_num = len(feature_cols)
# label = "total_inventory"
feature_num_col_list = feature_df.select(*feature_num_cols).distinct().collect()
res_dic = {}
best_parameter_dic = {}
s_time = time.time()
base_idx = 0
for i in range(len(feature_cols)):
    key = f"{base_idx}~{base_idx+feature_num_col_list[0][i]-1}"
    val = feature_cols[i].replace("_hot_vector", "")
    base_idx += feature_num_col_list[0][i]

selected_cols = ['date', 'content_id'] + feature_cols + label_cols
error_dic = {}
print(feature_cols)
print(feature_num_cols)
# feature_df.where('tournament in ("wc2022")').select('cross_features_2_hots_num', 'cross_features_2_hot_vector', 'if_contain_india_team_hots', 'vod_type_hots', 'tournament_type_hots').show(20, False)
# predict_feature_df.select('cross_features_2_hots_num', 'cross_features_2_hot_vector', 'if_contain_india_team_hots', 'vod_type_hots', 'tournament_type_hots').show(20, False)


if configuration['task'] == "rate_prediction":
    for test_tournament in tournament_list:
        res_dic[test_tournament] = {}
        if test_tournament not in configuration['test_tournaments']:
            continue
        print(test_tournament)
        tournament_list_tmp = tournament_list.copy()
        tournament_list_tmp.remove(test_tournament)
        object_method = "reg:squarederror"
        # object_method = "reg:logistic"
        # object_method = "reg:pseudohubererror"
        if configuration['mask_tag'] == "":
            train_df = load_dataset(tournament_list_tmp, feature_df)
            test_df = load_dataset([test_tournament], feature_df, sorting=True)
        else:
            train_df = load_dataset(tournament_list_tmp, feature_df, repeat_num_col=repeat_num_col,
                                    mask_cols=mask_cols, mask_condition=mask_condition, mask_rate=int(knock_off_repeat_num/2))
            test_df = load_dataset([test_tournament], feature_df, sorting=True,
                                   mask_cols=mask_cols, mask_condition=mask_condition, mask_rate=1)
        print(len(train_df))
        print(len(test_df))
        multi_col_df = []
        for i in range(len(feature_cols)):
            index = [feature_cols[i]+str(j) for j in range(feature_num_col_list[0][i])]
            multi_col_df.append(train_df[feature_cols[i]].apply(pd.Series, index=index))
        X_train = pd.concat(multi_col_df, axis=1)
        multi_col_df = []
        for i in range(len(feature_cols)):
            index = [feature_cols[i] + str(j) for j in range(feature_num_col_list[0][i])]
            multi_col_df.append(test_df[feature_cols[i]].apply(pd.Series, index=index))
        X_test = pd.concat(multi_col_df, axis=1)
        for label in label_cols:
            # print("")
            print(label)
            # ## for data enrichment
            # if configuration['data_enrichment']:
            #     train_df = load_dataset(tournament_list_tmp, feature_df)
            #     train_df = train_df.reindex(train_df.index.repeat(train_df[f"{label}_repeat_num"]))
            #     multi_col_df = []
            #     for i in range(len(feature_cols)):
            #         index = [feature_cols[i] + str(j) for j in range(feature_num_col_list[0][i])]
            #         multi_col_df.append(train_df[feature_cols[i]].apply(pd.Series, index=index))
            #     X_train = pd.concat(multi_col_df, axis=1)
            # ## for data enrichment
            if label not in res_dic:
                res_dic[label] = {}
            # for n_estimators in range(2, 5):
            for n_estimators in tree_num_list:
                # print(n_estimators)
                for learning_rate in learning_rate_list:
                    # for learning_rate in [0.1]:
                    for max_depth in max_depth_list:
                        # print(f"config: n_estimators={n_estimators}, max_depth={max_depth}")
                        y_train = train_df[label]
                        if configuration['model'] == "xgb":
                            model = XGBRegressor(base_score=0.0, n_estimators=n_estimators, learning_rate=learning_rate,
                                                 max_depth=max_depth, objective=object_method, colsample_bytree=1.0,
                                                 colsample_bylevel=1.0, colsample_bynode=colsample_bynode)
                        else:
                            model = RandomForestRegressor(n_estimators=n_estimators, max_depth=max_depth)
                        if configuration['sample_weight']:
                            model.fit(X_train, y_train, sample_weight=train_df[f"sample_repeat_num"])
                        else:
                            model.fit(X_train, y_train)
                        # y_pred = xgb.predict(X_train)
                        # y_mean = y_train.mean()
                        # error = metrics.mean_absolute_error(y_train, y_pred)
                        # train_error = error/y_mean
                        y_test = test_df[label]
                        y_pred = model.predict(X_test)
                        y_mean = y_test.mean()
                        error = metrics.mean_absolute_error(y_test, y_pred)
                        key = f"{object_method}-{n_estimators}-{learning_rate}-{max_depth}"
                        if key not in res_dic[label]:
                            res_dic[label][key] = [error / y_mean]
                        else:
                            res_dic[label][key].append(error / y_mean)
    best_setting_dic = {}
    for label in label_cols:
        error_list = []
        for key in res_dic[label]:
            error_list.append((key, sum(res_dic[label][key])/len(res_dic[label][key])))
        best_parameter_dic[label] = sorted(error_list, key=lambda x: x[1])[:2]
        # print(best_parameter_dic[label])
        print(res_dic[label][best_parameter_dic[label][0][0]])
        best_setting_dic[label] = best_parameter_dic[label][0][0].split("-")
    print(best_setting_dic)
    print(time.time() - s_time)
else:
    items = [([], ["ipl2022", "ac2022", "wc2022"], 1),
             (configuration['predict_tournaments'], [], 1)]
    # items = [([], ["wc2019"], 2)]
    for item in items:
        if configuration['prediction_free_timer'] < 1000 and item[0] == []:
            continue
        print(item)
        train_prediction_dic = {}
        train_error_dic = {}
        test_prediction_dic = {}
        test_error_dic = {}
        configuration['predict_tournaments'] = item[0]
        if configuration['predict_tournaments']:
            configuration['test_tournaments'] = configuration['predict_tournaments']
        else:
            # configuration['test_tournaments'] = ["wc2019", "wc2021", "ipl2022", "ac2022", "wc2022"]
            # configuration['wc2019_test_tag'] = 1
            configuration['test_tournaments'] = item[1]
            configuration['wc2019_test_tag'] = item[2]
        best_setting_dic = {'free_avg_session_num': ['reg:squarederror', '97', '0.2', '3'],
                            'sub_avg_session_num': ['reg:squarederror', '65', '0.1', '5']}
        idx = 0
        for label in best_setting_dic:
            train_prediction_dic[label] = []
            test_prediction_dic[label] = []
            best_setting_dic[label].append(estimated_label_cols[idx])
            idx += 1
        print(best_setting_dic)
        for test_tournament in tournament_list + configuration['predict_tournaments']:
            if test_tournament not in configuration['test_tournaments']:
                continue
            print(test_tournament)
            res_dic[test_tournament] = {}
            if test_tournament == "wc2019":
                sample_tag = configuration['wc2019_test_tag']
            else:
                sample_tag = 0
            tournament_list_tmp = tournament_list.copy()
            if test_tournament != "wc2019" and (test_tournament not in configuration['predict_tournaments']):
                tournament_list_tmp.remove(test_tournament)
            if test_tournament in configuration['predict_tournaments']:
                test_feature_df = predict_feature_df
            else:
                test_feature_df = feature_df
            test_tournament_list = [test_tournament]
            object_method = "reg:squarederror"
            # object_method = "reg:logistic"
            # object_method = "reg:pseudohubererror"
            if configuration['mask_tag'] == "":
                train_df = load_dataset(tournament_list_tmp, feature_df, test_tournament=test_tournament)
                test_df = load_dataset(test_tournament_list, test_feature_df, sorting=True, test_tournament=test_tournament, sub_or_free="free")
            else:
                train_df = load_dataset(tournament_list_tmp, feature_df, repeat_num_col="knock_off_repeat_num",
                                        mask_cols=mask_cols, mask_condition=mask_condition, mask_rate=int(knock_off_repeat_num/2), test_tournament=test_tournament)
                test_df = load_dataset(test_tournament_list, test_feature_df, repeat_num_col="", sorting=True,
                                       mask_cols=mask_cols, mask_condition=mask_condition, mask_rate=1, test_tournament=test_tournament)
            # ## for data enrichment
            # train_df = train_df.reindex(train_df.index.repeat(train_df[f"knock_off_repeat_num"]))
            print(len(train_df))
            test_num = len(test_df)
            print(test_num)
            # print(test_df)
            # ## for data enrichment
            multi_col_df = []
            for i in range(len(feature_cols)):
                index = [feature_cols[i]+str(j) for j in range(feature_num_col_list[0][i])]
                multi_col_df.append(train_df[feature_cols[i]].apply(pd.Series, index=index))
            X_train = pd.concat(multi_col_df, axis=1)
            multi_col_df = []
            for i in range(len(feature_cols)):
                index = [feature_cols[i] + str(j) for j in range(feature_num_col_list[0][i])]
                multi_col_df.append(test_df[feature_cols[i]].apply(pd.Series, index=index))
            X_test = pd.concat(multi_col_df, axis=1)
            train_error_dic[test_tournament] = []
            test_error_dic[test_tournament] = []
            for label in label_cols:
                # print("")
                print(label)
                # for prediction start
                object_method, n_estimators, learning_rate, max_depth, col_name = best_setting_dic[label]
                n_estimators = int(n_estimators)
                learning_rate = float(learning_rate)
                max_depth = int(max_depth)
                y_train = train_df[label]
                if configuration['model'] == "xgb":
                    model = XGBRegressor(base_score=0.0, n_estimators=n_estimators, learning_rate=learning_rate,
                                         max_depth=max_depth, objective=object_method, colsample_bytree=1.0,
                                         colsample_bylevel=1.0, colsample_bynode=colsample_bynode)
                else:
                    model = RandomForestRegressor(n_estimators=n_estimators, max_depth=max_depth)
                if configuration['sample_weight']:
                    model.fit(X_train, y_train, sample_weight=train_df[f"sample_repeat_num"])
                else:
                    model.fit(X_train, y_train)
                # for tree_idx in range(n_estimators)[:1]:
                #     plot_tree(model, num_trees=tree_idx)
                #     plt.savefig(f"{test_tournament}-{label}-{tree_idx}.png", dpi=600)
                if configuration['task'] == "prediction_result_save":
                    y_pred = model.predict(X_test)
                    y_test = test_df[label]
                    prediction_df = spark\
                        .createDataFrame(pd.concat([test_df[['date', 'content_id']], y_test, pd.DataFrame(y_pred)], axis=1), ['date', 'content_id', 'real_'+label, col_name])\
                        .withColumn(col_name, F.expr(f'cast({col_name} as float)'))
                    # prediction_df.orderBy('date', 'content_id').show(10, False)
                    save_data_frame(prediction_df,
                                    preroll_live_ads_inventory_forecasting_root_path
                                    +f"/xgb_prediction{configuration['mask_tag']}{configuration['au_source'].replace('avg', '').replace('_au', '')}{prediction_vod_str}/{test_tournament}/{label}/sample_tag={sample_tag}")
                    error = metrics.mean_absolute_error(y_test, y_pred)
                    y_mean = y_test.mean()
                    print(error / y_mean)
                    if test_tournament not in error_dic:
                        error_dic[test_tournament] = []
                    error_dic[test_tournament].append((error, y_mean, test_num))
                    res_dic[test_tournament][label] = error / y_mean
                else:
                    y_pred = model.predict(X_train)
                    prediction_df = spark.createDataFrame(pd.concat([train_df[['date', 'content_id']], y_train, pd.DataFrame(y_pred)], axis=1), ['date', 'content_id', 'real_' + label, col_name]) \
                        .withColumn(col_name, F.expr(f'cast({col_name} as float)'))\
                        .withColumn('test_tournament', F.lit(test_tournament))\
                        .withColumn('label', F.lit(label_cols.index(label)))
                    train_prediction_dic[label].append(prediction_df)
                    error = metrics.mean_absolute_error(y_train, y_pred)
                    y_mean = y_train.mean()
                    train_error_dic[test_tournament].append(error / y_mean)
                    y_pred = model.predict(X_test)
                    y_test = test_df[label]
                    prediction_df = spark.createDataFrame(pd.concat([test_df[['date', 'content_id']], y_test, pd.DataFrame(y_pred)], axis=1), ['date', 'content_id', 'real_'+label, col_name])\
                        .withColumn(col_name, F.expr(f'cast({col_name} as float)'))\
                        .withColumn('test_tournament', F.lit(test_tournament))\
                        .withColumn('label', F.lit(label_cols.index(label)))
                    test_prediction_dic[label].append(prediction_df)
                    # save_data_frame(prediction_df, live_ads_inventory_forecasting_root_path+f"/xgb_prediction{mask_tag}/{test_tournament}/{label}")
                    error = metrics.mean_absolute_error(y_test, y_pred)
                    y_mean = y_test.mean()
                    test_error_dic[test_tournament].append(error / y_mean)
                    if test_tournament not in error_dic:
                        error_dic[test_tournament] = []
                    error_dic[test_tournament].append((error, y_mean, test_num))
                    # res_dic[test_tournament][label] = error / y_mean
                    # ## for prediction end
        if configuration['task'] != "prediction_result_save":
            # train_error_df = reduce(lambda x, y: x.join(y, ['date', 'content_id', 'test_tournament']), [df.drop('label') for df in train_error_list[:len(label_cols)]]).orderBy('date', 'content_id').cache()
            # train_error_df2 = reduce(lambda x, y: x.join(y, ['date', 'content_id', 'test_tournament']), [df.drop('label') for df in train_error_list[(len(label_cols))*(-1):]]).orderBy('date', 'content_id').cache()
            # test_error_df = reduce(lambda x, y: x.join(y, ['date', 'content_id', 'test_tournament']), [df.drop('label') for df in test_error_list[:len(label_cols)]]).orderBy('date', 'content_id').cache()
            # test_error_df2 = reduce(lambda x, y: x.join(y, ['date', 'content_id', 'test_tournament']), [df.drop('label') for df in test_error_list[(len(label_cols))*(-1):]]).orderBy('date', 'content_id').cache()
            prediction_result_list = []
            print(train_error_dic)
            print(test_error_dic)
            for tournament in test_error_dic:
                print(tournament)
                for error in test_error_dic[tournament]:
                    print(error)
            for label in label_cols:
                prediction_result_list.append(reduce(lambda x, y: x.union(y), [df.drop('label') for df in test_prediction_dic[label]]))
            reduce(lambda x, y: x.join(y, ['date', 'content_id', 'test_tournament']),
                   [df for df in prediction_result_list])\
                .orderBy('date', 'content_id')\
                .show(2000)
            # train_error_df2.orderBy('date', 'content_id').show(2000)
            # test_error_df2.orderBy('date', 'content_id').show(2000)
        else:
            for tournament in res_dic:
                print(tournament)
                print(res_dic[tournament])
    if configuration['task'] == "prediction_result_save":
        for label in label_cols:
            all_tournaments = configuration['predict_tournaments']
            if configuration['prediction_free_timer'] == 1000:
                all_tournaments += ["ipl2022", "ac2022", "wc2022"]
            version = "baseline_with_predicted_parameters"
            df = reduce(lambda x, y: x.union(y), [load_data_frame(spark, preroll_live_ads_inventory_forecasting_root_path
                                +f"/xgb_prediction{configuration['mask_tag']}{configuration['au_source'].replace('avg', '').replace('_au', '')}{prediction_vod_str}/{test_tournament}/{label}")
                                                 for test_tournament in all_tournaments])
            df.orderBy('date', 'content_id').show(2000)


