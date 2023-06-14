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

storageLevel = StorageLevel.DISK_ONLY
distribution_fun = "default"
# distribution_fun = "gaussian"
valid_creative_dic = {}


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
        return spark.read.option("mergeSchema", "true").parquet(path)
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


concurrency_root_path = "s3://hotstar-dp-datalake-processed-us-east-1-prod/hive_internal_database/concurrency.db/"
ssai_concurrency_path = f"{concurrency_root_path}/users_by_live_sports_content_by_ssai"
match_meta_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/ads_crash/match_meta"
live_ads_inventory_forecasting_root_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting"
play_out_log_input_path = "s3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test/"
impression_path = "s3://hotstar-data-lake-northvirginia/data/source/campaignTracker/parquet_bifrost/impression_events"
watch_video_path = "s3://hotstar-dp-datalake-processed-us-east-1-prod/events/watched_video/"
watch_video_sampled_path = "s3://hotstar-ads-ml-us-east-1-prod/data_exploration/data/data_backup/watched_video/"
play_out_log_v2_input_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/playout_v2/cd='
play_out_log_v3_input_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/playout_v3/'
play_out_log_original_input_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/playout_original/cd="
watchAggregatedInputPath = "s3://hotstar-dp-datalake-processed-us-east-1-prod/aggregates/watched_video_daily_aggregates_ist_v4"

# df = spark.read.parquet(watch_video_path+"/cd=2022-11-13")
#
# df\
#     .select('received_at', 'timestamp')\
#     .withColumn('timestamp', F.substring(F.col('timestamp'), 1, 19))\
#     .withColumn('diff', F.unix_timestamp(F.col('received_at'), 'yyyy-MM-dd HH:mm:ss')-F.unix_timestamp(F.col('timestamp'), 'yyyy-MM-dd HH:mm:ss'))\
#     .groupBy('diff').count().orderBy('count', ascending=False).show(200)


# spark.stop()
# spark = hive_spark('statistics')
# match_df = load_hive_table(spark, "in_cms.match_update_s3")
# save_data_frame(match_df, match_meta_path)
tournament_dic = {"wc2022": ['"ICC Men\'s T20 World Cup 2022"'],
                  "ac2022": ['"DP World Asia Cup 2022"'],
                  "ipl2022": ['"TATA IPL 2022"'],
                  "wc2021": ['"ICC Men\'s T20 World Cup 2021"'],
                  "wc2019": ['"ICC CWC 2019"'],
                  "ipl2021": ['"VIVO IPL 2021"'],
                  "ipl2020": ['"Dream11 IPL 2020"'],
                  "west_indies_tour_of_india2019": ['"West Indies Tour India 2019"', '"West Indies Tour of India 2019"'],
                  "australia_tour_of_india2020": ['"Australia Tour of India 2020"', '"Australia tour of India 2020"'],
                  "india_tour_of_new_zealand2020": ['"India Tour of New Zealand 2020"', '"India tour of New Zealand 2020"'],
                  "england_tour_of_india2021": ['"England Tour of India 2021"'],
                  "south_africa_tour_of_india2022": ['"South Africa Tour of India 2022"'],
                  "west_indies_tour_of_india2022": ['"West Indies Tour of India 2022"'],
                  "sri_lanka_tour_of_india2023": ['"Sri Lanka Tour of India 2023"'],
                  "new_zealand_tour_of_india2023": ['"New Zealand Tour of India 2023"']}

tournament_list = [tournament for tournament in tournament_dic]
content_id_col = "Content ID"
start_time_col = "Start Time"
end_time_col = "End Time"
break_duration_col = "Delivered Time"
content_language_col = "Language"
platform_col = "Platform"
tenant_col = "Tenant"
creative_id_col = "Creative ID"
break_id_col = "Break ID"
playout_id_col = "Playout ID"
creative_path_col = "Creative Path"
content_id_col2 = "_c4"
start_time_col2 = "_c2"
end_time_col2 = "_c13"
break_duration_col2 = "_c15"
content_language_col2 = "_c5"
platform_col2 = "_c8"
tenant_col2 = "_c6"
creative_id_col2 = "_c9"
break_id_col2 = "_c10"
playout_id_col2 = "_c3"
creative_path_col2 = "_c11"
content_language_col3 = "_c1"


def get_match_data(tournament):
    match_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"match_data/{tournament}").cache()
    print(f"match number: {match_df.count()}")
    valid_dates = match_df.select('date').orderBy('date').distinct().collect()
    # match_df.show(200, False)
    if tournament == "ipl2021":
        valid_dates = [date for date in valid_dates if date[0] >= "2021-09-21" and date[0] != "2021-09-26" and date[0] != "2021-10-10"][:-1]
    if tournament == "england_tour_of_india2021":
        valid_dates = [date for date in valid_dates if date[0] >= "2021-03-12"]
    if tournament == "ipl2020":
        valid_dates_tmp = ["2020-09-19", "2020-09-20", "2020-09-21", "2020-09-22", "2020-09-23", "2020-09-24", "2020-09-25", "2020-09-26", "2020-09-27", "2020-09-28", "2020-09-29", "2020-09-30",
                           "2020-10-01", "2020-10-02", "2020-10-05", "2020-10-06", "2020-10-07", "2020-10-08", "2020-10-09", "2020-10-12", "2020-10-19", "2020-10-21", "2020-10-23", "2020-10-26",
                           "2020-11-08"]
        valid_dates = [date for date in valid_dates if date[0] in valid_dates_tmp]
    print(valid_dates)
    complete_valid_dates = [date[0] for date in valid_dates]
    for date in valid_dates:
        next_date = get_date_list(date[0], 2)[-1]
        if next_date not in complete_valid_dates:
            complete_valid_dates.append(next_date)
    print(complete_valid_dates)
    return match_df, valid_dates, complete_valid_dates


# conn = psycopg2.connect(database='ad_model',
#                         user='ads_user',
#                         password='ads_pass',
#                         host="blaze-ad-model-rds-aur-cls-prod-ind-ap-southeast-1.cluster-cka7qtmp9a6h.ap-southeast-1.rds.amazonaws.com",
#                         port=5432)
# cursor = conn.cursor()
# creative_df = load_blaze_pg_table(cursor, "creative", ['creative_id', 'id']) \
#     .selectExpr('creative_id', 'id').distinct()
# asset_df = load_blaze_pg_table(cursor, "asset", ['creative_id', 'duration']) \
#     .selectExpr('creative_id as id', 'duration')\
#     .where('duration is not null and duration > 0')\
#     .withColumn('duration', F.expr('duration/1000'))\
#     .distinct()
# cursor.close()
# creative_df = creative_df\
#     .join(creative_df.groupBy('creative_id').count().where('count = 1').drop('count'), 'creative_id')
# asset_df = asset_df\
#     .join(asset_df.groupBy('id').count().where('count = 1').drop('count'), 'id')
# duration_df = creative_df.join(asset_df, 'id').drop('id').cache()
#
# impression_df = reduce(lambda x, y: x.union(y),
#                     [load_data_frame(spark, f"{impression_path}/cd={date[0]}")
#                     .withColumn('date', F.lit(date[0])) for date in valid_dates])\
#     .where('pdf5 = "BlazeVAST"')\
#     .selectExpr('date', 'content_id', 'pdf1 as break_id', 'dw_p_id', 'dw_d_id', 'ad_id as creative_id')\
#     .withColumn('break_no', F.substring(F.col('break_id'), -3, 3))\
#     .withColumn('break_no', F.expr('cast(break_no as int)'))\
#     .join(F.broadcast(duration_df), 'creative_id', 'left')\
#     .fillna('17.5', ['duration'])\
#     .cache()
# #
# # # for reach calculation
# # match_df.join(impression_df
# #               .groupBy('date', 'content_id')
# #               .agg(F.countDistinct("dw_p_id").alias('total_pid_reach'),
# #                    F.countDistinct("dw_d_id").alias('total_did_reach')),
# #               ['date', 'content_id'], 'left')\
# #     .orderBy('date', 'content_id')\
# #     .show(200, False)
#
#
# # for inventory calculation
# match_df.join(impression_df
#               .groupBy('date', 'content_id')
#               .agg(F.count("*").alias('total_impression'),
#                    F.sum('duration').alias('total_impression_per_10_seconds'))
#               .withColumn('total_impression_per_10_seconds', F.expr('cast(total_impression_per_10_seconds / 10 as int)')),
#               ['date', 'content_id'], 'left')\
#     .orderBy('date', 'content_id')\
#     .show(200, False)
#
#
# save_data_frame(impression_df\
#     .groupBy('date', 'content_id', 'break_no')\
#     .agg(F.count("*").alias('total_impression'), F.sum('duration').alias('total_duration')), live_ads_inventory_forecasting_root_path + f"/test_dataset/break_level_impression_of_{tournament}")
#
# impression_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/test_dataset/break_level_impression_of_{tournament}")
#
# match_df.join(impression_df
#               .where("break_no <= 25")
#               .groupBy('date', 'content_id')
#               .agg(F.sum('total_impression').alias('total_impression'),
#                    F.sum('total_duration').alias('total_impression_per_10_seconds'))
#               .withColumn('total_impression', F.expr('total_impression * 2'))
#               .withColumn('total_impression_per_10_seconds', F.expr('cast(total_impression_per_10_seconds * 2 / 10 as int)')),
#               ['date', 'content_id'], 'left')\
#     .orderBy('date', 'content_id')\
#     .show(200, False)

# output_level = "tournament"

output_folder = "/tournament_level"

# print(output_folder)
# match_reach_list = []
# tournament_reach_list = []
# # tournament_list = ["wc2019"]
# for tournament in tournament_list:
#     if tournament in ["wc2019"]:
#         continue
#     print(tournament)
#     match_df, valid_dates, complete_valid_dates = get_match_data(tournament)
#     final_playout_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/test_dataset/break_start_time_data_1_of_{tournament}") \
#         .select('content_id').distinct()\
#         .where('content_id not in ("1440000689", "1440000694", "1440000696", "1440000982", '
#                '"1540017172", "1540017175", "1540019005", "1540019014", "1540019017","1540016333")')\
#         .cache()
#     print(final_playout_df.count())
#     df = reduce(lambda x, y: x.union(y),
#            [load_data_frame(spark, f'{watchAggregatedInputPath}/cd={date}', fmt="orc")
#            .withColumnRenamed('_col1', 'dw_d_id')
#            .withColumnRenamed('_col2', 'content_id')
#            .select('content_id', 'dw_d_id') for date in complete_valid_dates]) \
#         .join(F.broadcast(final_playout_df), ['content_id']) \
#         .withColumn('tournament', F.lit(tournament)) \
#         .cache()
#     res_df = df\
#         .groupBy('tournament')\
#         .agg(F.countDistinct('dw_d_id').alias('total_reach'), F.countDistinct('content_id').alias('match_num'))\
#         .join(df.groupBy('tournament', 'content_id').agg(F.countDistinct('dw_d_id').alias('match_reach')), 'tournament') \
#         .withColumn('reach_rate', F.expr('match_reach/total_reach'))\
#         .cache()
#     save_data_frame(res_df, live_ads_inventory_forecasting_root_path + output_folder + f"/content_reach_rate/{tournament}")
#     res_df.show(200, False)
#     spark.catalog.clearCache()
#



data_source = "watched_video"
tournament = "wc2019"
match_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/final_test_dataset/{data_source}_of_{tournament}")\
        .withColumn('tournament', F.lit(tournament))\
        .selectExpr('tournament', 'content_id', 'total_did_reach as match_reach')
tournament_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + output_folder + f"/final_test_dataset/{data_source}_of_{tournament}") \
    .selectExpr('tournament', 'match_num', 'total_did_reach as total_reach')
spark.conf.set("spark.sql.crossJoin.enabled", "true")
wc2019_df = match_df\
    .join(tournament_df, 'tournament')\
    .where('match_reach > 0')\
    .withColumn('reach_rate', F.expr('match_reach/total_reach'))\
    .cache()

wc2019_df.orderBy('content_id').show(100, False)
save_data_frame(wc2019_df, live_ads_inventory_forecasting_root_path + output_folder + f"/content_reach_rate/{tournament}")


df = reduce(lambda x, y: x.union(y), [load_data_frame(spark, live_ads_inventory_forecasting_root_path + output_folder + f"/content_reach_rate/{tournament}")
            .select('content_id', 'match_reach', 'total_reach', 'reach_rate', 'tournament', 'match_num') for tournament in tournament_list])\
    .cache()

df.orderBy('content_id').show(1000, False)
df.count()
save_data_frame(df, live_ads_inventory_forecasting_root_path + output_folder + f"/content_reach_rate/all")

