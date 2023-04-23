import datetime
import os
import sys
from functools import reduce
import pyspark.sql.functions as F
from pyspark.shell import spark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
from common import load_requests

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
            .select('date', 'content_id', 'title')\
            .withColumn('total_inventory', F.lit(2023))\
            .withColumn('total_pid_reach', F.lit(-1))\
            .withColumn('total_did_reach', F.lit(-1))
    else:
        df = load_data_frame(spark, pipeline_base_path + f"/label/inventory/tournament={tournament}")
    # date, content_id, title,
    # total_inventory, total_pid_reach, total_did_reach
    return df.select('date', 'content_id', 'total_inventory', 'total_pid_reach', 'total_did_reach')


def free_timer_wt(wt_list):
    if len(wt_list) == 1:
        return float(wt_list[0])
    else:
        jio_rate = 0.75
        wt_list = sorted(wt_list)
        return (1 - jio_rate) * wt_list[0] + jio_rate * wt_list[1]


free_timer_wt_udf = F.udf(free_timer_wt, FloatType())


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

predict_tournament = "wc2023"
dau_prediction_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_v3/forecast/"
sub_pid_did_rate = 0.94
free_pid_did_rate = 1.02
today = str(datetime.date.today())


def load_dataset(config):
    path_suffix = "/all_features_hots_format_and_simple_one_hot"
    all_feature_df = load_data_frame(spark, pipeline_base_path + path_suffix) \
        .withColumn('tag', F.lit(1)) \
        .cache()
    if config == {}:
        predict_feature_df = load_data_frame(spark, live_ads_inventory_forecasting_complete_feature_path + f"/{predict_tournament}/all_features_hots_format") \
            .cache()
    else:
        base_path_suffix = "/prediction/all_features_hots_format_and_simple_one_hot"
        predict_feature_df = load_data_frame(spark, pipeline_base_path + base_path_suffix + f"/cd={today}") \
            .cache()
    common_cols = list(set(all_feature_df.columns).intersection(set(predict_feature_df.columns)))
    all_feature_df = all_feature_df.select(*common_cols)\
        .union(predict_feature_df.select(*common_cols))\
        .withColumn('tag', F.lit(1))\
        .cache()
    estimated_dau_df = all_feature_df\
        .selectExpr('tournament', 'total_frees_number as estimated_free_num', 'total_subscribers_number as estimated_sub_num')\
        .distinct()\
        .where(f'estimated_free_num > 0 and estimated_sub_num > 0')\
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
    return all_feature_df, estimated_dau_df


def main(version, mask_tag, config={}):
    all_feature_df, estimated_dau_df = load_dataset(config)
    if config == {}:
        test_tournaments = ["wc2019", "wc2021", "ipl2022", "ac2022", "wc2022", "wc2023"]
        parameter_path = ""
    else:
        test_tournaments = []
        for tournament in config:
            test_tournaments.append(tournament['seasonName'].replace(" ", "_").lower())
        parameter_path = f"future_tournaments/cd={today}/"
    res_list = []
    for test_tournament in test_tournaments:
        test_feature_df = all_feature_df \
            .where(f"tournament='{test_tournament}'") \
            .selectExpr('content_id', 'title', 'rank', 'teams', 'tournament', 'match_stage',
                        'total_frees_number', 'active_frees_rate as real_active_frees_rate',
                        'frees_watching_match_rate as real_frees_watching_match_rate',
                        'watch_time_per_free_per_match as real_watch_time_per_free_per_match',
                        'total_subscribers_number', 'active_subscribers_rate as real_active_subscribers_rate',
                        'subscribers_watching_match_rate as real_subscribers_watching_match_rate',
                        'watch_time_per_subscriber_per_match as real_watch_time_per_subscriber_per_match') \
            .cache()
        test_label_df = load_labels(f"{test_tournament}", all_feature_df) \
            .join(test_feature_df, 'content_id') \
            .cache()
        print(test_tournament)
        test_df = test_label_df \
            .join(estimated_dau_df, 'tournament') \
            .cache()
        if version in ["baseline_with_predicted_parameters"]:
            label_cols = ['frees_watching_match_rate', "watch_time_per_free_per_match",
                          'subscribers_watching_match_rate', "watch_time_per_subscriber_per_match"]
            label_path = f"{pipeline_base_path}/xgb_prediction{mask_tag}/{parameter_path}tournament={test_tournament}"
            # print(first_match_date)
            new_test_label_df = test_df \
                .withColumn('estimated_variables', F.lit(0)) \
                .join(load_data_frame(spark, f"{label_path}/label={label_cols[0]}")
                    .drop('sample_tag', 'real_' + label_cols[0]), ['date', 'content_id']) \
                .join(load_data_frame(spark, f"{label_path}/label={label_cols[2]}")
                    .drop('sample_tag', 'real_' + label_cols[2]), ['date', 'content_id']) \
                .join(load_data_frame(spark, f"{label_path}/label={label_cols[3]}")
                    .drop('sample_tag', 'real_' + label_cols[3]), ['date', 'content_id']) \
                .cache()
            label = 'watch_time_per_free_per_match_with_free_timer'
            parameter_df = load_data_frame(spark, f"{label_path}/label={label}") \
                .drop('sample_tag', 'real_' + label) \
                .groupBy('date', 'content_id') \
                .agg(F.collect_list('estimated_watch_time_per_free_per_match_with_free_timer').alias('estimated_watch_time_per_free_per_match')) \
                .withColumn('estimated_watch_time_per_free_per_match', free_timer_wt_udf('estimated_watch_time_per_free_per_match')) \
                .cache()
            new_test_label_df = new_test_label_df \
                .join(parameter_df, ['date', 'content_id']) \
                .cache()
            for configuration in configurations[1:2]:
                total_match_duration_in_minutes, number_of_ad_breaks, average_length_of_a_break_in_seconds = configuration
                res_df = new_test_label_df \
                    .withColumn('real_avg_concurrency', F.expr(
                    f'(total_frees_number * real_frees_watching_match_rate * real_watch_time_per_free_per_match '
                    f'+ total_subscribers_number * real_subscribers_watching_match_rate * real_watch_time_per_subscriber_per_match)'
                    f'/{total_match_duration_in_minutes}')) \
                    .withColumn('estimated_avg_concurrency', F.expr(
                    f'(estimated_free_num * estimated_frees_watching_match_rate * estimated_watch_time_per_free_per_match '
                    f'+ estimated_sub_num * estimated_subscribers_watching_match_rate * estimated_watch_time_per_subscriber_per_match)/{total_match_duration_in_minutes}')) \
                    .withColumn('estimated_inventory', F.expr(
                    f'estimated_avg_concurrency * {drop_off_rate} * ({number_of_ad_breaks * average_length_of_a_break_in_seconds} / 10.0)')) \
                    .withColumn('estimated_reach', F.expr(
                    f"(estimated_free_num * estimated_frees_watching_match_rate / {free_pid_did_rate}) + (estimated_sub_num * estimated_subscribers_watching_match_rate / {sub_pid_did_rate})")) \
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
                print(final_cols)
                res_df = res_df.select(*final_cols).orderBy('date', 'content_id')
                save_data_frame(res_df, pipeline_base_path + f"/inventory_prediction{mask_tag}/{parameter_path}test_tournament={test_tournament}")
                res_list.append(res_df.withColumn('tournament', F.lit(test_tournament)))
        print("")
        print("")
    return res_list


version = "baseline_with_predicted_parameters"
mask_tag = ""
# mask_tag = "_mask_knock_off"
DATE=sys.argv[1]
config = load_requests(DATE)
res_list = main(version=version, mask_tag=mask_tag, config=config)
tournament_dic = {
    "wc2023": -1,
    "wc2022": 0,
    "ac2022": 1,
    "ipl2022": 2,
    "wc2021": 3,
    "wc2019": 4,
}
tag_mapping_udf = F.udf(lambda x: tournament_dic[x] if x in tournament_dic else 0, IntegerType())
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
    .withColumn('total_error', F.expr('(estimated_inventory - total_inventory) / total_inventory')) \
    .withColumn('total_match_error', F.expr('sum_inventory_abs_error / total_inventory')) \
    .withColumn('tag', tag_mapping_udf('tournament')) \
    .orderBy('tag')\
    .drop('tag')\
    .show(100, False)


