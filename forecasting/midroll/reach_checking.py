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


def save_data_frame(df: DataFrame, path: str, fmt: str = 'parquet', header: bool = False, delimiter: str = ',', partition_col = '') -> None:
    def save_data_frame_internal(df: DataFrame, path: str, fmt: str = 'parquet', header: bool = False,
                                 delimiter: str = ',') -> None:
        if fmt == 'parquet':
            if partition_col == "":
                df.write.mode('overwrite').parquet(path)
            else:
                df.write.partitionBy(partition_col).mode('overwrite').parquet(path)
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


def check_title_valid(title, *args):
    for arg in args:
        if title.find(arg) > -1:
            return 0
    if title.find(' vs ') == -1:
        return 0
    return 1


def calculate_sub_num_on_target_date(sub_df, user_meta_df, target_date):
    sub_df = sub_df\
        .where(f'sub_start_time <= "{target_date}" and sub_end_time >= "{target_date}"')\
        .select('hid')\
        .distinct()\
        .join(user_meta_df, 'hid')
    return sub_df.select('dw_p_id').distinct()


check_title_valid_udf = F.udf(check_title_valid, IntegerType())


concurrency_root_path = "s3://hotstar-dp-datalake-processed-us-east-1-prod/hive_internal_database/concurrency.db/"
ssai_concurrency_path = f"{concurrency_root_path}/users_by_live_sports_content_by_ssai"
match_meta_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/ads_crash/match_meta"
live_ads_inventory_forecasting_root_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting"
play_out_log_input_path = "s3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test"
watchAggregatedInputPath = "s3://hotstar-dp-datalake-processed-us-east-1-prod/aggregates/watched_video_daily_aggregates_ist_v4"
viewAggregatedInputPath = "s3://hotstar-dp-datalake-processed-us-east-1-prod/aggregates/viewed_page_daily_aggregates_ist_v2"
live_ads_inventory_forecasting_complete_feature_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/complete_features"
watch_video_path = "s3://hotstar-dp-datalake-processed-us-east-1-prod/events/watched_video/"
watch_video_sampled_path = "s3://hotstar-ads-ml-us-east-1-prod/data_exploration/data/data_backup/watched_video/"

# spark.stop()
# spark = SparkSession.builder \
#         .appName("test") \
#         .config("hive.metastore.uris", "thrift://metastore.data.hotstar-labs.com:9083") \
#         .config("spark.kryoserializer.buffer.max", "128m") \
#         .enableHiveSupport() \
#         .getOrCreate()
#
# q1 = """select cms_sports_season_name, content_id, cms_title, count(distinct dw_device_id), count(distinct dw_d_id), count(distinct dw_p_id)
# from data_warehouse.watched_video_daily_aggregates_ist a
# where cd between date '2019-05-30' and date '2019-07-14'
# and cms_content_type = 'SPORT_LIVE'
# and cms_sports_season_name = 'ICC Cricket World Cup 2019'
# group by cms_sports_season_name, content_id, cms_title"""
# df = spark.sql(q1)
# df.show(100, False)

# tournament = "wc2019"
# rate = 1
# aggr_col = "tournament"
# for data_source in ["watched_video", "watched_video_sampled"]:
#     print(data_source)
#     load_data_frame(spark, live_ads_inventory_forecasting_root_path+f"/backup/{data_source}_of_{tournament}")\
#         .withColumn('tournament', F.lit(tournament)) \
#         .where('content_id <= "1440000700"')\
#         .groupBy(aggr_col) \
#         .agg(F.countDistinct("dw_d_id").alias('content_reach'), F.sum('watch_time').alias('watch_time'), F.countDistinct('content_id')) \
#         .withColumn('content_reach', F.expr(f'content_reach*{rate}')) \
#         .withColumn('watch_time', F.expr(f'watch_time*{rate}')) \
#         .orderBy(aggr_col)\
#         .show(1000, False)
#     rate = 4

rate = 4
filter_str = "if(tag in ('2', '8', 'a', 'e'), 0, if(tag in ('0', '4', '7', 'c'), 1, if(tag in ('1', '6', '9', 'd'), 2, 3)))"
for wd_path in [watch_video_path]:
    print(wd_path)
    reduce(lambda x, y: x.union(y), [load_data_frame(spark, f"{wd_path}/cd={date}").select('dw_p_id', 'dw_d_id', 'watch_time', 'dw_device_id') for date in get_date_list("2023-03-16", 30)]) \
        .withColumn('tournament', F.lit("2023"))\
        .withColumn('tag', F.col("dw_p_id").substr(-1, 1))\
        .withColumn('tag_set', F.expr(filter_str))\
        .groupBy('tag_set')\
        .agg(F.countDistinct('dw_p_id').alias('content_reach_p'),
             F.countDistinct('dw_d_id').alias('content_reach_d'),
             F.countDistinct('dw_device_id').alias('content_reach_device'),
             F.sum('watch_time').alias('watch_time')) \
        .withColumn('content_reach_p', F.expr(f'content_reach_p*{rate}')) \
        .withColumn('content_reach_d', F.expr(f'content_reach_d*{rate}')) \
        .withColumn('content_reach_device', F.expr(f'content_reach_device*{rate}')) \
        .withColumn('watch_time', F.expr(f'watch_time*{rate}')) \
        .show(1000, False)
    # rate = 4




