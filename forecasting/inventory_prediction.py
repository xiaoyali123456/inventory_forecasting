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

storageLevel = StorageLevel.DISK_ONLY


def check_s3_path_exist(s3_path: str) -> bool:
    if not s3_path.endswith("/"):
        s3_path += "/"
    return os.system(f"aws s3 ls {s3_path}_SUCCESS") == 0


def check_s3_folder_exist(s3_path: str) -> bool:
    if not s3_path.endswith("/"):
        s3_path += "/"
    return os.system(f"aws s3 ls {s3_path}") == 0


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


def load_labels(tournament, feature_df):
    if not check_s3_folder_exist(pipeline_base_path + f"/label/inventory/tournament={tournament}"):
        df = feature_df\
            .where(f"tournament='{tournament}'")\
            .select('date', 'content_id', 'title', 'shortsummary')\
            .withColumn('total_inventory', F.lit(2023))\
            .withColumn('total_pid_reach', F.lit(-1))\
            .withColumn('total_did_reach', F.lit(-1))
    else:
        df = load_data_frame(spark, pipeline_base_path + f"/label/inventory/tournament={tournament}")
    # date, content_id, title, shortsummary,
    # total_inventory, total_pid_reach, total_did_reach
    return df.select('date', 'content_id', 'total_inventory', 'total_pid_reach', 'total_did_reach')


def first_match_data(df, col):
    min_rank = df.select(F.min('rank')).collect()[0][0]
    return df.where(f'rank = {min_rank}').select(col).collect()[0][0]


def last_match_data(df, col):
    max_rank = df.select(F.max('rank')).collect()[0][0]
    return df.where(f'rank = {max_rank}').select(col).collect()[0][0]


def days_bewteen_st_and_et(st, et):
    date1 = datetime.datetime.strptime(st, "%Y-%m-%d").date()
    date2 = datetime.datetime.strptime(et, "%Y-%m-%d").date()
    return (date2 - date1).days


def get_first_match_data_and_increasing_rate(previous_tournament, tag_tournament_1,
                                             tag_tournament_2, base_tournament,
                                             col, base_tournament_days, test_df):
    first_match_num = last_match_data(all_feature_df.where(f"tournament='{previous_tournament}'"), col) + \
                      ((first_match_data(all_feature_df.where(f"tournament='{tag_tournament_2}'"), col) -
                        last_match_data(all_feature_df.where(f"tournament='{tag_tournament_1}'"), col)) /
                       days_bewteen_st_and_et(
                           last_match_data(all_feature_df.where(f"tournament='{tag_tournament_1}'"), 'date'),
                           first_match_data(all_feature_df.where(f"tournament='{tag_tournament_2}'"), 'date'))) \
                      * days_bewteen_st_and_et(
        last_match_data(all_feature_df.where(f"tournament='{previous_tournament}'"), 'date'),
        first_match_data(test_df, 'date'))
    num_increasing_rate = (last_match_data(all_feature_df.where(f"tournament='{base_tournament}'"), col)
                           - first_match_data(all_feature_df.where(f"tournament='{base_tournament}'"),
                                              col)) / base_tournament_days
    return first_match_num, num_increasing_rate


concurrency_root_path = "s3://hotstar-dp-datalake-processed-us-east-1-prod/hive_internal_database/concurrency.db/"
ssai_concurrency_path = f"{concurrency_root_path}/users_by_live_sports_content_by_ssai"
match_meta_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/ads_crash/match_meta"
live_ads_inventory_forecasting_root_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting"
play_out_log_input_path = "s3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test"
watchAggregatedInputPath = "s3://hotstar-dp-datalake-processed-us-east-1-prod/aggregates/watched_video_daily_aggregates_ist_v4"
viewAggregatedInputPath = "s3://hotstar-dp-datalake-processed-us-east-1-prod/aggregates/viewed_page_daily_aggregates_ist_v2"
live_ads_inventory_forecasting_complete_feature_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/complete_features"
pipeline_base_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/pipeline"

# total_match_duration_in_minutes, number_of_ad_breaks, average_length_of_a_break_in_seconds = 325.0, 55.0, 80.0
# total_match_duration_in_minutes, number_of_ad_breaks, average_length_of_a_break_in_seconds = 210.0, 85.0, 30.0
# total_match_duration_in_minutes, number_of_ad_breaks, average_length_of_a_break_in_seconds = 210.0, 45.0, 55.0
configurations = [(210.0, 55.0, 80.0), (210.0, 85.0, 30.0), (210.0, 45.0, 55.0)]
# drop_off_rate = 0.8
drop_off_rate = 0.85
dynamic_parameters = ['active_frees_rate', 'frees_watching_match_rate', "watch_time_per_free_per_match",
                      'active_subscribers_rate', 'subscribers_watching_match_rate',
                      "watch_time_per_subscriber_per_match"]
dynamic_parameter_num = len(dynamic_parameters)
one_hot_cols = ['tournament_type', 'if_weekend', 'match_time', 'if_holiday', 'venue', 'if_contain_india_team',
                'match_type', 'tournament_name', 'hostar_influence',
                'match_stage', 'vod_type', 'gender_type']
multi_hot_cols = ['teams', 'continents', 'teams_tier']
additional_cols = ["languages", "platforms"]
invalid_cols = ['gender_type']
feature_cols = [col + "_hot_vector" for col in one_hot_cols[:-1] + multi_hot_cols + additional_cols]
feature_num = len(feature_cols)
top_N_matches = 5
# test_tournament = "wc2022"
# test_tournament = "ac2022"
# test_tournament = "wc2021"
# test_tournament = "ipl2022"
# version = "baseline"
# version = "save_free_and_sub_number_predictions"
# sub_version = 3

predict_tournament = "wc2023"
dau_prediction_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_predict/DAU_predict.parquet"
sub_pid_did_rate = 0.94
free_pid_did_rate = 1.02

path_suffix = "/all_features_hots_format"
all_feature_df = load_data_frame(spark, pipeline_base_path + path_suffix) \
    .withColumn('tag', F.lit(1)) \
    .cache()

predict_feature_df = load_data_frame(spark, live_ads_inventory_forecasting_complete_feature_path + f"/{predict_tournament}/all_features_hots_format")\
    .cache()

common_cols = list(set(all_feature_df.columns).intersection(set(predict_feature_df.columns)))

all_feature_df = all_feature_df.select(*common_cols)\
    .union(predict_feature_df.select(*common_cols))\
    .withColumn('tag', F.lit(1))\
    .cache()

# if not check_s3_path_exist(live_ads_inventory_forecasting_complete_feature_path + "/dau_prediction"):
#     dau_df = load_data_frame(spark, dau_prediction_path) \
#         .withColumn('estimated_free_num', F.expr('DAU - subs_DAU')) \
#         .selectExpr('cd as date', 'estimated_free_num', 'subs_DAU as estimated_sub_num') \
#         .join(all_feature_df.select('date', 'tournament').distinct(), 'date') \
#         .groupBy('tournament') \
#         .agg(F.avg('estimated_free_num').alias('estimated_free_num'),
#              F.avg('estimated_sub_num').alias('estimated_sub_num'))
#     save_data_frame(dau_df, live_ads_inventory_forecasting_complete_feature_path + "/dau_prediction")

estimated_dau_df = all_feature_df\
    .selectExpr('tournament', 'total_frees_number as estimated_free_num', 'total_subscribers_number as estimated_sub_num')\
    .distinct()\
    .where(f'tournament != "{predict_tournament}"')\
    .union(load_data_frame(spark, dau_prediction_path)
           .withColumn('estimated_free_num', F.expr('DAU - subs_DAU'))
           .selectExpr('cd as date', 'estimated_free_num', 'subs_DAU as estimated_sub_num')
           .join(predict_feature_df.select('date', 'tournament').distinct(), 'date')
           .groupBy('tournament')
           .agg(F.avg('estimated_free_num').alias('estimated_free_num'),
                F.avg('estimated_sub_num').alias('estimated_sub_num'))
           .selectExpr('tournament', 'estimated_free_num', 'estimated_sub_num'))\
    .cache()

print(estimated_dau_df.count())
estimated_dau_df.orderBy('tournament').show(20, False)

feature_df = all_feature_df.where('tournament != "ipl2019"').cache()
tournament_df = all_feature_df \
    .groupBy('tournament') \
    .agg(F.min('date').alias('date')) \
    .orderBy('date') \
    .select('tournament') \
    .collect()
tournament_list = [item[0] for item in tournament_df]
print(tournament_list)
tournament_idx_dic = {}
match_type_list = ["t20", "odi", "test"]
matching_features_list = ['match_type', 'vod_type', 'tournament_type', 'tournament_name']
for idx in range(len(tournament_list)):
    tournament_idx_dic[tournament_list[idx]] = idx


def main(version, mask_tag):
    res_list = []
    for test_tournament in ["wc2019", "wc2021", "ipl2022", "ac2022", "wc2022", "wc2023"]:
        # for test_tournament in ["ac2022", "wc2022"]:
        # for test_tournament in ["wc2019"]:
        # for test_tournament in ["wc2023"]:
        if test_tournament == "ipl2019":
            continue
        test_feature_df = feature_df \
            .where(f"tournament='{test_tournament}'") \
            .selectExpr('content_id', 'title', 'shortsummary', 'rank', 'teams', 'tournament', *matching_features_list, 'match_stage',
                        'total_frees_number', 'active_frees_rate as real_active_frees_rate',
                        'frees_watching_match_rate as real_frees_watching_match_rate',
                        'watch_time_per_free_per_match as real_watch_time_per_free_per_match',
                        'total_subscribers_number', 'active_subscribers_rate as real_active_subscribers_rate',
                        'subscribers_watching_match_rate as real_subscribers_watching_match_rate',
                        'watch_time_per_subscriber_per_match as real_watch_time_per_subscriber_per_match') \
            .cache()
        test_match_type_list = test_feature_df.select('match_type').distinct().collect()
        # print(test_match_type_list)
        test_label_df = load_labels(f"{test_tournament}", all_feature_df) \
            .join(test_feature_df, 'content_id') \
            .cache()
        for match_type in test_match_type_list:
            print(test_tournament)
            print(match_type[0])
            test_df = test_label_df \
                .where(f'match_type="{match_type[0]}"') \
                .cache()
            test_df = test_df \
                .join(estimated_dau_df, 'tournament') \
                .cache()
            if version in ["baseline_with_predicted_parameters"]:
                label_cols = ['frees_watching_match_rate', "watch_time_per_free_per_match",
                              'subscribers_watching_match_rate', "watch_time_per_subscriber_per_match"]
                # print(first_match_date)
                new_test_label_df = test_df \
                    .withColumn('estimated_variables', F.lit(0)) \
                    .join(load_data_frame(spark,  pipeline_base_path + f"/xgb_prediction{mask_tag}/{test_tournament}/{label_cols[0]}")
                    .drop('sample_tag', 'real_' + label_cols[0]), ['date', 'content_id']) \
                    .join(load_data_frame(spark, pipeline_base_path + f"/xgb_prediction{mask_tag}/{test_tournament}/{label_cols[1]}").drop(
                    'sample_tag', 'real_' + label_cols[1]), ['date', 'content_id']) \
                    .join(load_data_frame(spark, pipeline_base_path + f"/xgb_prediction{mask_tag}/{test_tournament}/{label_cols[2]}").drop(
                    'sample_tag', 'real_' + label_cols[2]), ['date', 'content_id']) \
                    .join(load_data_frame(spark, pipeline_base_path + f"/xgb_prediction{mask_tag}/{test_tournament}/{label_cols[3]}").drop(
                    'sample_tag', 'real_' + label_cols[3]), ['date', 'content_id']) \
                    .cache()
                for configuration in configurations[1:2]:
                    total_match_duration_in_minutes, number_of_ad_breaks, average_length_of_a_break_in_seconds = configuration
                    res_df = new_test_label_df \
                        .withColumn('real_avg_concurrency', F.expr(
                        f'(total_frees_number * real_frees_watching_match_rate * real_watch_time_per_free_per_match '
                        f'+ total_subscribers_number * real_subscribers_watching_match_rate * real_watch_time_per_subscriber_per_match)'
                        f'/{total_match_duration_in_minutes}')) \
                        .withColumn('estimated_avg_concurrency', F.expr(
                        f'(estimated_free_num * estimated_free_watch_rate * estimated_free_watch_time '
                        f'+ estimated_sub_num * estimated_sub_watch_rate * estimated_sub_watch_time)/{total_match_duration_in_minutes}')) \
                        .withColumn('estimated_inventory', F.expr(
                        f'estimated_avg_concurrency * {drop_off_rate} * ({number_of_ad_breaks * average_length_of_a_break_in_seconds} / 10.0)')) \
                        .withColumn('estimated_reach', F.expr(
                        f"(estimated_free_num * estimated_free_watch_rate / {free_pid_did_rate}) + (estimated_sub_num * estimated_sub_watch_rate / {sub_pid_did_rate})")) \
                        .withColumn('estimated_inventory', F.expr('cast(estimated_inventory as bigint)')) \
                        .withColumn('estimated_reach', F.expr('cast(estimated_reach as bigint)')) \
                        .withColumn('avg_concurrency_bias',
                                    F.expr('(estimated_avg_concurrency - real_avg_concurrency) / real_avg_concurrency')) \
                        .withColumn('reach_bias', F.expr('(estimated_reach - total_did_reach) / total_did_reach')) \
                        .withColumn('reach_bias_abs', F.expr('abs(reach_bias)')) \
                        .withColumn('inventory_bias', F.expr('(estimated_inventory - total_inventory) / total_inventory')) \
                        .withColumn('inventory_bias_abs', F.expr('abs(estimated_inventory - total_inventory)')) \
                        .withColumn('inventory_bias_abs_rate', F.expr('inventory_bias_abs / total_inventory')) \
                        .where('total_inventory > 0') \
                        .drop('teams') \
                        .cache()
                    cols = res_df.columns
                    important_cols = ["real_avg_concurrency", "estimated_avg_concurrency", "avg_concurrency_bias",
                                      "total_did_reach", 'estimated_reach', "reach_bias",
                                      "total_inventory", "estimated_inventory", "inventory_bias", 'inventory_bias_abs']
                    for col in important_cols:
                        cols.remove(col)
                    final_cols = cols + important_cols
                    # print(final_cols)
                    res_df = res_df.select(*final_cols).orderBy('date', 'content_id')
                    save_data_frame(res_df, pipeline_base_path + f"/test_result_of_{test_tournament}_using_{version}{mask_tag}")
                    # res_df.select('date', 'title', 'estimated_inventory').show(200, False)
                    # res_df.show(200, False)
                    # print(configuration)
                    # res_df \
                    #     .groupBy('shortsummary') \
                    #     .agg(F.sum('total_inventory').alias('total_inventory'),
                    #          F.sum('estimated_inventory').alias('estimated_inventory')) \
                    #     .withColumn('bias', F.expr('(estimated_inventory - total_inventory) / total_inventory')) \
                    #     .show(200, False)
                    # res_df \
                    #     .groupBy('shortsummary') \
                    #     .agg(F.sum('real_avg_concurrency').alias('real_avg_concurrency'),
                    #          F.sum('estimated_avg_concurrency').alias('estimated_avg_concurrency')) \
                    #     .withColumn('bias', F.expr('(estimated_avg_concurrency - real_avg_concurrency) / real_avg_concurrency')) \
                    #     .show(200, False)
                    res_list.append(res_df \
                                    .withColumn('tournament', F.lit(test_tournament))
                                    .withColumn('match_type', F.lit(match_type[0])))
        print("")
        print("")
    return res_list


version = "baseline_with_predicted_parameters"
# mask_tag = ""
mask_tag = "_mask_knock_off"
res_list = main(version=version, mask_tag=mask_tag)
tournament_dic = {
    "wc2023": -1,
    "wc2022": 0,
    "ac2022": 1,
    "ipl2022": 2,
    "wc2021": 3,
    "wc2019": 4,
}
tag_mapping_udf = F.udf(lambda x: tournament_dic[x], IntegerType())
reduce(lambda x, y: x.union(y), res_list) \
    .groupBy('tournament') \
    .agg(F.sum('real_avg_concurrency').alias('real_avg_concurrency'),
         F.sum('estimated_avg_concurrency').alias('estimated_avg_concurrency'),
         F.sum('total_inventory').alias('total_inventory'),
         F.sum('estimated_inventory').alias('estimated_inventory'),
         F.avg('inventory_bias_abs_rate').alias('avg_match_error'),
         F.sum('inventory_bias_abs').alias('sum_inventory_abs_error'),
         F.avg('reach_bias_abs').alias('avg_reach_bias_abs'),
         F.count('content_id')) \
    .withColumn('concurrency_bias', F.expr('(estimated_avg_concurrency - real_avg_concurrency) / real_avg_concurrency')) \
    .withColumn('total_error', F.expr('(estimated_inventory - total_inventory) / total_inventory')) \
    .withColumn('total_match_error', F.expr('sum_inventory_abs_error / total_inventory')) \
    .withColumn('tag', tag_mapping_udf('tournament')) \
    .orderBy('tag')\
    .drop('tag')\
    .show(100, False)