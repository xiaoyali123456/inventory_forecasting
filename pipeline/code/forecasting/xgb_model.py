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
from xgboost import plot_tree
import matplotlib.pyplot as plt
# pip3 install --upgrade holidays
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


def load_dataset(feature_df, test_tournament, sorting=False, repeat_num_col="", mask_cols=[], mask_condition="", mask_rate=1, wc2019_test_tag=1):
    if test_tournament == "wc2019":
        if sorting:
            sample_tag = wc2019_test_tag
        else:
            sample_tag = 3 - wc2019_test_tag
            # sample_tag = "1, 2"
    else:
        sample_tag = "1, 2"
    if sorting:
        new_feature_df = feature_df \
            .where(f'tournament = "{test_tournament}" and sample_tag in (0, {sample_tag})') \
            .cache()
    else:
        new_feature_df = feature_df \
            .where(f'tournament != "{test_tournament}" and sample_tag in (0, {sample_tag})') \
            .cache()
        if test_tournament == "wc2019":
            new_feature_df = new_feature_df.union(feature_df.where(f'tournament = "{test_tournament}" and sample_tag in (0, {sample_tag})')).cache()
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


def cross_features(dim, *args):
    res = [0 for i in range(dim)]
    idx = 1
    for x in args:
        idx *= x[0] + 1
    res[idx-1] = 1
    return res


def empty_cross_features(dim, *args):
    res = [0 for i in range(dim)]
    idx = 3
    for x in args:
        idx *= x[0] + 1
    res[idx-1] = 1
    return res


def generate_hot_vector(hots, hots_num):
    res = [0 for i in range(hots_num)]
    for hot in hots:
        res[hot] += 1
    return res


def feature_processing(feature_df):
    match_rank_df = feature_df \
        .select('content_id', 'tournament', 'match_stage', 'rand') \
        .distinct() \
        .withColumn("match_rank", F.expr('row_number() over (partition by tournament, match_stage order by rand)')) \
        .select('content_id', 'match_rank') \
        .cache()
    return feature_df \
        .withColumn(repeat_num_col, F.expr(f'if({mask_condition}, {knock_off_repeat_num}, 1)')) \
        .withColumn("hostar_influence_hots_num", F.lit(1)) \
        .withColumn("if_contain_india_team_hots_num", F.lit(2)) \
        .withColumn("if_contain_india_team_hots",
                    if_contain_india_team_hot_vector_udf('if_contain_india_team_hots', 'tournament_type')) \
        .withColumn("if_contain_india_team_hot_vector",
                    generate_hot_vector_udf('if_contain_india_team_hots', 'if_contain_india_team_hots_num')) \
        .join(match_rank_df, 'content_id') \
        .withColumn("knock_rank", F.expr('row_number() over (partition by tournament order by date desc)')) \
        .withColumn("knock_rank", F.expr('if(match_stage not in ("final", "semi-final"), 100, knock_rank)')) \
        .withColumn("knock_rank_hot_vector", F.array(F.col('knock_rank'))) \
        .withColumn("knock_rank_hots_num", F.lit(1)) \
        .withColumn("sample_tag", F.expr('if(tournament="wc2019", if(match_stage="group", if(rand<0.5, 1, 2), if(match_rank<=1, 1, 2)), 0)')) \
        .withColumn("sub_tag", F.array(F.lit(1))) \
        .withColumn("free_tag", F.array(F.lit(0))) \
        .withColumn("sub_free_tag_hots_num", F.lit(1)) \
        .withColumn('sample_repeat_num', F.expr('if(content_id="1440000724", 2, 1)'))\
        .where('date != "2022-08-24" and tournament != "ipl2019"')


n_to_array = F.udf(lambda n: [n] * n, ArrayType(IntegerType()))
mask_array = F.udf(lambda l: [0 for i in l], ArrayType(IntegerType()))
enumerate_tiers_udf = F.udf(enumerate_tiers, ArrayType(IntegerType()))
order_match_stage_udf = F.udf(lambda x: [match_stage_dic[x]], ArrayType(IntegerType()))
if_contain_india_team_hot_vector_udf = F.udf(lambda x, y: x if y != "national" else [1], ArrayType(IntegerType()))
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
pipeline_base_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/pipeline"

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
    # 'mask_tag': "mask_knock_off",
    'predict_tournaments_candidate': ["wc2023"],
    'mask_tag': "",
    # 'sample_weight': False,
    'sample_weight': True,
    'unvalid_labels': ['active_frees_rate', 'active_subscribers_rate'],
    # 'unvalid_labels': [],
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
    # 'cross_features': [['if_contain_india_team_hot_vector', 'match_stage_hots', 'tournament_type_hots']],
    'cross_features': [['if_contain_india_team_hots', 'match_stage_hots', 'tournament_type_hots'],
                       ['if_contain_india_team_hots', 'match_type_hots', 'tournament_type_hots']],
    'if_make_match_stage_ranked': False,
    # 'if_make_match_stage_ranked': True,
    'end_tag': 0
}
match_stage_dic = {
    'qualifier': 0,
    'group': 1,
    'semi-final': 2,
    'final': 3
}
hots_num_dic = {
    'if_contain_india_team_hots': 3,
    'match_stage_hots': 4,
    'tournament_type_hots': 3,
    'match_type_hots': 3,
    'vod_type_hots': 2
}
knock_off_repeat_num = 1
repeat_num_col = "knock_off_repeat_num"
mask_condition = 'match_stage in ("final", "semi-final")'
mask_cols = []
today = str(datetime.date.today())


def load_train_and_prediction_dataset(config, if_free_timer):
    base_label_cols = ['active_frees_rate', 'frees_watching_match_rate', "watch_time_per_free_per_match",
                       'active_subscribers_rate', 'subscribers_watching_match_rate', "watch_time_per_subscriber_per_match"]
    if if_free_timer == "":
        label_cols = base_label_cols
    else:
        label_cols = ["watch_time_per_free_per_match_with_free_timer"]
    path_suffix = "/all_features_hots_format" + if_free_timer + configuration['if_simple_one_hot']
    feature_df = feature_processing(load_data_frame(spark, pipeline_base_path + path_suffix))\
        .cache()
    if config == {}:
        predict_feature_df = feature_processing(reduce(lambda x, y: x.union(y), [load_data_frame(spark, live_ads_inventory_forecasting_complete_feature_path + "/" + tournament
                                                      + "/all_features_hots_format" + configuration['if_simple_one_hot']) for tournament in configuration['predict_tournaments_candidate']])\
            .withColumn("rand", F.rand(seed=54321)))\
            .cache()
    else:
        base_path_suffix = "/prediction/all_features_hots_format" + if_free_timer + configuration['if_simple_one_hot']
        predict_feature_df = feature_processing(load_data_frame(spark, pipeline_base_path + base_path_suffix + f"/cd={today}").withColumn("rand", F.rand(seed=54321))) \
            .cache()
    one_hot_cols = ['tournament_type', 'if_weekend', 'match_time', 'if_holiday', 'venue', 'if_contain_india_team',
                    'match_type', 'tournament_name', 'hostar_influence',
                    'match_stage', 'vod_type']
    multi_hot_cols = ['teams', 'continents', 'teams_tier']
    additional_cols = ["languages", "platforms"]
    if not configuration['if_hotstar_influence']:
        one_hot_cols.remove('hostar_influence')
    if not configuration['if_teams']:
        multi_hot_cols.remove('teams')
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
            feature_dim = reduce(lambda x, y: x * y, [hots_num_dic[feature] for feature in configuration['cross_features'][idx]])
            # print(feature_dim)
            feature_df = feature_df \
                .withColumn(f"cross_features_{idx}_hots_num", F.lit(feature_dim)) \
                .withColumn(f"cross_features_{idx}_hot_vector", cross_features_udf(f"cross_features_{idx}_hots_num", *configuration['cross_features'][idx])) \
                .withColumn(f"empty_cross_features_{idx}_hot_vector", empty_cross_features_udf(f"cross_features_{idx}_hots_num", *(configuration['cross_features'][idx][1:]))) \
                .cache()
            predict_feature_df = predict_feature_df \
                .withColumn(f"cross_features_{idx}_hots_num", F.lit(feature_dim)) \
                .withColumn(f"cross_features_{idx}_hot_vector", cross_features_udf(f"cross_features_{idx}_hots_num", *configuration['cross_features'][idx])) \
                .withColumn(f"empty_cross_features_{idx}_hot_vector", empty_cross_features_udf(f"cross_features_{idx}_hots_num", *(configuration['cross_features'][idx][1:]))) \
                .cache()
            one_hot_cols = [f'cross_features_{idx}'] + one_hot_cols
            mask_features.append(f'cross_features_{idx}')
            # feature_df.select(f"cross_features_{idx}_hots_num", f"cross_features_{idx}_hot_vector").show(20, False)
    global mask_cols
    mask_cols = [col+"_hot_vector" for col in multi_hot_cols + mask_features]
    feature_cols = [col+"_hot_vector" for col in one_hot_cols+multi_hot_cols]
    if configuration['if_contains_language_and_platform']:
        feature_cols += [col+"_hot_vector" for col in additional_cols]
    if if_free_timer != "":
        feature_df = feature_df \
            .withColumn("free_timer_hot_vector", F.array(F.col('free_timer'))) \
            .withColumn("free_timer_hots_num", F.lit(1)) \
            .cache()
        predict_feature_df = predict_feature_df \
            .withColumn("free_timer_hot_vector", F.array(F.lit(1000))) \
            .withColumn("free_timer_hots_num", F.lit(1)) \
            .withColumn('watch_time_per_free_per_match_with_free_timer', F.lit(1.0)) \
            .cache()
        feature_cols += ["free_timer_hot_vector"]
    if configuration['if_make_match_stage_ranked']:
        feature_df = feature_df \
            .withColumn("match_stage_hot_vector", order_match_stage_udf('match_stage')) \
            .withColumn("match_stage_hots_num", F.lit(1)) \
            .cache()
        predict_feature_df = predict_feature_df \
            .withColumn("match_stage_hot_vector", order_match_stage_udf('match_stage')) \
            .withColumn("match_stage_hots_num", F.lit(1)) \
            .cache()
    return feature_df, predict_feature_df, feature_cols, label_cols


def model_prediction(test_tournaments, feature_df, predict_feature_df, feature_cols, label_cols, mask_tag="", wc2019_test_tag=1, config={}):
    res_dic = {}
    s_time = time.time()
    train_error_list2 = {}
    test_error_list2 = {}
    feature_num_cols = [col.replace("_hot_vector", "_hots_num") for col in feature_cols]
    feature_num_col_list = feature_df.select(*feature_num_cols).distinct().collect()
    labeled_tournaments = [item[0] for item in feature_df.select('tournament').distinct().collect()]
    print(label_cols)
    best_setting_dic = {'active_frees_rate': ['reg:squarederror', '93', '0.1', '9'],
                        'frees_watching_match_rate': ['reg:squarederror', '45', '0.05', '11'],
                        'watch_time_per_free_per_match': ['reg:squarederror', '73', '0.05', '3'],
                        'active_subscribers_rate': ['reg:squarederror', '37', '0.2', '9'],
                        'subscribers_watching_match_rate': ['reg:squarederror', '53', '0.05', '9'],
                        'watch_time_per_subscriber_per_match': ['reg:squarederror', '61', '0.1', '3'],
                        'watch_time_per_free_per_match_with_free_timer': ['reg:squarederror', '73', '0.05', '5']}
    for test_tournament in test_tournaments:
        print(test_tournament)
        res_dic[test_tournament] = {}
        if test_tournament not in labeled_tournaments:
            test_feature_df = predict_feature_df
        else:
            test_feature_df = feature_df
        if mask_tag == "":
            train_df = load_dataset(feature_df, test_tournament, wc2019_test_tag=wc2019_test_tag)
            test_df = load_dataset(test_feature_df, test_tournament, wc2019_test_tag=wc2019_test_tag, sorting=True)
        else:
            train_df = load_dataset(feature_df, test_tournament, wc2019_test_tag=wc2019_test_tag,
                                    repeat_num_col="knock_off_repeat_num",
                                    mask_cols=mask_cols, mask_condition=mask_condition,
                                    mask_rate=int(knock_off_repeat_num / 2))
            test_df = load_dataset(feature_df, test_tournament, wc2019_test_tag=wc2019_test_tag, sorting=True,
                                   mask_cols=mask_cols, mask_condition=mask_condition, mask_rate=1)
        print(len(train_df))
        print(len(test_df))
        multi_col_df = []
        for i in range(len(feature_cols)):
            index = [feature_cols[i] + str(j) for j in range(feature_num_col_list[0][i])]
            multi_col_df.append(train_df[feature_cols[i]].apply(pd.Series, index=index))
        X_train = pd.concat(multi_col_df, axis=1)
        multi_col_df = []
        for i in range(len(feature_cols)):
            index = [feature_cols[i] + str(j) for j in range(feature_num_col_list[0][i])]
            multi_col_df.append(test_df[feature_cols[i]].apply(pd.Series, index=index))
        X_test = pd.concat(multi_col_df, axis=1)
        train_error_list2[test_tournament] = []
        test_error_list2[test_tournament] = []
        for label in label_cols:
            # print("")
            if label in configuration['unvalid_labels']:
                continue
            print(label)
            # for prediction start
            object_method, n_estimators, learning_rate, max_depth = best_setting_dic[label]
            n_estimators = int(n_estimators)
            learning_rate = float(learning_rate)
            max_depth = int(max_depth)
            y_train = train_df[label]
            if configuration['model'] == "xgb":
                model = XGBRegressor(base_score=0.0, n_estimators=n_estimators, learning_rate=learning_rate,
                                     max_depth=max_depth, objective=object_method, colsample_bytree=1.0,
                                     colsample_bylevel=1.0, colsample_bynode=1.0)
            else:
                model = RandomForestRegressor(n_estimators=n_estimators, max_depth=max_depth)
            if configuration['sample_weight']:
                model.fit(X_train, y_train, sample_weight=train_df["sample_repeat_num"])
            else:
                model.fit(X_train, y_train)
            y_pred = model.predict(X_test)
            y_test = test_df[label]
            prediction_df = spark.createDataFrame(
                pd.concat([test_df[['date', 'content_id']], y_test, pd.DataFrame(y_pred)], axis=1),
                ['date', 'content_id', 'real_' + label, 'estimated_' + label]) \
                .withColumn('estimated_' + label, F.expr(f'cast({"estimated_" + label} as float)'))
            if config == {}:
                save_data_frame(prediction_df, pipeline_base_path + f"/xgb_prediction{mask_tag}/{test_tournament}/{label}/sample_tag={wc2019_test_tag}")
            else:
                save_data_frame(prediction_df, pipeline_base_path + f"/xgb_prediction{mask_tag}/future_tournaments/{today}/{test_tournament}/{label}/sample_tag={wc2019_test_tag}")
            error = metrics.mean_absolute_error(y_test, y_pred)
            y_mean = y_test.mean()
            print(error / y_mean)
            res_dic[test_tournament][label] = error / y_mean
    print(res_dic)


def main(config={}, free_time_tag=""):
    feature_df, predict_feature_df, feature_cols, label_cols = load_train_and_prediction_dataset(config, if_free_timer=free_time_tag)
    if config == {}:
        items = [(["wc2019", "wc2021", "ipl2022", "ac2022", "wc2022"], 1),
                 (["wc2019"], 2),
                 (["wc2023"], 1)]
        for item in items:
            model_prediction(item[0], feature_df, predict_feature_df, feature_cols, label_cols, mask_tag="",
                             wc2019_test_tag=item[1], config=config)
    else:
        test_tournaments = []
        for tournament in config['results']:
            test_tournaments.append(tournament['seasonName'].replace(" ", "_").lower())
        print(test_tournaments)
        model_prediction(test_tournaments, feature_df, predict_feature_df, feature_cols, label_cols, mask_tag="", config=config)


config = {
    "results": [
        {
            "id": "123_586",
            "tournamentId": 123,
            "tournamentName": "World Cup",
            "seasonId": 586,
            "seasonName": "World Cup 2023",
            "requestStatus": "INIT",
            "tournamentType": "AVOD",
            "svodFreeTimeDuration": 5,
            "svodSubscriptionPlan": "free",
            "sportType": "CRICKET",
            "tournamentLocation": "India",
            "matchDetails": [
                {
                    "matchId": 1235,
                    "matchName": "",
                    "tournamentCategory": "ODI",
                    "estimatedMatchDuration": 420,
                    "matchDate": "2023-10-14",
                    "matchStartHour": 14,
                    "matchType": "group",
                    "teams": [
                        {
                            "name": "India",
                            "tier": "tier1"
                        },
                        {
                            "name": "Australia",
                            "tier": "tier1"
                        }
                    ],
                    "fixedBreak": 50,
                    "averageBreakDuration": 45,
                    "fixedAdPodsPerBreak": [
                    ],
                    "adhocBreak": 30,
                    "adhocBreakDuration": 10,
                    "publicHoliday": "false",
                    "contentLanguage": "",
                    "PlatformSuported": [
                        "android",
                        "IOS",
                        "web"
                    ]
                }
            ],
            "customAudienes": [
                {
                    "uploadSource": "ap_tool",
                    "segmentName": "AP_567",
                    "customCohort": "C_14_1",
                    "upload_date": "2023-02-26"
                }
            ],
            "adPlacements": [
                {
                    "adPlacement": "MIDROLL",
                    "version": 1,
                    "forecastSize": 15236523
                },
                {
                    "adPlacement": "PREROLL",
                    "version": 2,
                    "forecastSize": 551236265
                }
            ],
            "tournamentStartDate": "2023-02-26",
            "tournamentEndDate": "2023-04-26",
            "userName": "Navin Kumar",
            "emailId": "navin.kumar@hotstar.com",
            "creationDate": "2021-04-08T06:08:45.717+00:00",
            "lastModifiedDate": "2021-04-08T06:08:45.717+00:00",
            "error": ""
        }
    ],
    "current_page": 1,
    "total_items": 1,
    "total_pages": 1
}
# main()
main(config=config)
main(config=config, free_time_tag="_and_free_timer")
