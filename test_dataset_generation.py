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
import psycopg2

storageLevel = StorageLevel.DISK_ONLY
distribution_fun = "default"
# distribution_fun = "gaussian"
valid_creative_dic = {}


def check_s3_path_exist(s3_path: str) -> bool:
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


def check_title_valid(title, *args):
    for arg in args:
        if title.find(arg) > -1:
            return 0
    if title.find(' vs ') == -1:
        return 0
    return 1


def load_blaze_pg_table(cursor, table_name, cols):
    cursor.execute(f"select {', '.join(cols)} from {table_name}")
    res_tuples = cursor.fetchall()
    return spark.createDataFrame(res_tuples, cols)


def filter_users(df: DataFrame):
    user_last_char = ['2', 'a', 'e', '8']
    return df.filter(F.col("dw_p_id").substr(-1, 1).isin(user_last_char))


def avg_value(v1, v2, v3):
    res = 0
    count = 0
    for v in [v1, v2, v3]:
        if v > 0:
            res += v
            count += 1
    if count == 0:
        return 0
    return int(res/count)


check_title_valid_udf = F.udf(check_title_valid, IntegerType())
avg_value_udf = F.udf(avg_value, LongType())

concurrency_root_path = "s3://hotstar-dp-datalake-processed-us-east-1-prod/hive_internal_database/concurrency.db/"
ssai_concurrency_path = f"{concurrency_root_path}/users_by_live_sports_content_by_ssai"
match_meta_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/ads_crash/match_meta"
live_ads_inventory_forecasting_root_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting"
play_out_log_input_path = "s3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test"
impression_path = "s3://hotstar-data-lake-northvirginia/data/source/campaignTracker/parquet_bifrost/impression_events"
watch_video_path = "s3://hotstar-dp-datalake-processed-us-east-1-prod/events/watched_video/"
# watch_video_path = "s3://hotstar-ads-ml-us-east-1-prod/data_exploration/data/data_backup/watched_video/"

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
tournament_dic = {"wc2022": "ICC Men\'s T20 World Cup 2022",
                  "ipl2022": "TATA IPL 2022",
                  "wc2021": "ICC Men\'s T20 World Cup 2021",
                  "ipl2021": "VIVO IPL 2021"}
tournament = "wc2022"
# tournament = "ipl2022"
# tournament = "wc2021"
# tournament = "ipl2021"
tournament_list = ["wc2022", "wc2021", "ipl2022", "ipl2021"]
if tournament == "wc2022":
    content_id_col = "Content ID"
    start_time_col = "Start Time"
    end_time_col = "End Time"
    break_duration_col = "Delivered Time"
    content_language_col = "Language"
    platform_col = "Platform"
    tenant_col = "Tenant"
else:
    content_id_col = "_c4"
    start_time_col = "_c2"


match_df = load_data_frame(spark, match_meta_path)\
    .withColumn('date', F.expr('substring(from_unixtime(startdate), 1, 10)'))\
    .where(f'shortsummary="{tournament_dic[tournament]}" and contenttype="SPORT_LIVE"')\
    .withColumn('date', F.expr('if(contentid="1540019056", "2022-11-06", date)'))\
    .withColumn('title', F.expr('lower(title)'))\
    .withColumn('title_valid_tag', check_title_valid_udf('title', F.lit('warm-up'), F.lit('follow on')))\
    .where('title_valid_tag = 1')\
    .selectExpr('date', 'contentid as content_id', 'title', 'shortsummary')\
    .orderBy('date')\
    .distinct()\
    .cache()

valid_dates = match_df.select('date').distinct().collect()
print(valid_dates)
print(len(valid_dates))
print(match_df.count())
# match_df.show(200, False)
if tournament == "ipl2021":
    valid_dates = [date for date in valid_dates if date[0] >= "2021-09-21" and date[0] != "2021-09-26"][:-1]

print(valid_dates)

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


# for inventory calculation from concurrency and playout data
# playout_df = reduce(lambda x, y: x.union(y),
#                     [load_data_frame(spark, f"{play_out_log_input_path}/{date[0]}", 'csv', True)
#                     .withColumn('date', F.lit(date[0])) for date in valid_dates]) \
#     .withColumnRenamed(content_id_col, 'content_id') \
#     .withColumnRenamed(start_time_col, 'start_time') \
#     .withColumnRenamed(end_time_col, 'end_time') \
#     .withColumnRenamed(break_duration_col, 'delivered_duration') \
#     .withColumnRenamed(content_language_col, 'content_language') \
#     .withColumnRenamed(platform_col, 'platform') \
#     .withColumnRenamed(tenant_col, 'tenant') \
#     .withColumn('content_language', F.expr('lower(content_language)'))\
#     .withColumn('platform', F.expr('lower(platform)'))\
#     .withColumn('tenant', F.expr('lower(tenant)'))\
#     .withColumn('content_id', F.trim(F.col('content_id'))) \
#     .withColumn('start_time', F.expr('if(length(start_time)==8, start_time, from_unixtime(unix_timestamp(start_time, "hh:mm:ss aa"), "HH:mm:ss"))')) \
#     .withColumn('end_time', F.expr('if(length(end_time)==8, end_time, from_unixtime(unix_timestamp(end_time, "hh:mm:ss aa"), "HH:mm:ss"))')) \
#     .withColumn('delivered_duration', F.expr('cast(unix_timestamp(delivered_duration, "HH:mm:ss") as long)')) \
#     .where('content_id != "Content ID" and content_id is not null and start_time is not null')\
#     .cache()

# playout_df.where('content_id="1540018966" and content_language="hindi" and platform="android" and tenant="in"')\
#     .orderBy('start_time')\
#     .show(200, False)
#
# playout_df.where('content_id="1540018966"')\
#     .withColumn('start_time', F.expr('substring(start_time, 1, 5)'))\
#     .select('start_time')\
#     .distinct()\
#     .orderBy('start_time')\
#     .show(200, False)
#
# playout_df\
#     .join(match_df, ['date', 'content_id'])\
#     .groupBy('content_language', 'platform', 'tenant')\
#     .agg(F.count('start_time').alias('break_num'),
#         F.countDistinct('content_id').alias('match_num'),
#         F.avg('delivered_duration').alias('avg_break_duration'),
#         F.sum('delivered_duration').alias('total_break_duration'))\
#     .withColumn('avg_break_duration_per_match', F.expr('total_break_duration/match_num'))\
#     .orderBy('break_num')\
#     .show(200, False)
# +----------------+---------------------+------+---------+---------+------------------+--------------------+----------------------------+
# |content_language|platform             |tenant|break_num|match_num|avg_break_duration|total_break_duration|avg_break_duration_per_match|
# +----------------+---------------------+------+---------+---------+------------------+--------------------+----------------------------+
# |english         |na                   |ca    |2        |1        |604.5             |1209                |1209.0                      |
# |hindi           |ios|mweb|jiolyf|web  |in    |142      |1        |13.598591549295774|1931                |1931.0                      |
# |hindi           |na                   |ca    |152      |7        |72.28947368421052 |10988               |1569.7142857142858          |
# |english         |na                   |ca    |164      |8        |81.32926829268293 |13338               |1667.25                     |
# |hindi           |na                   |ca    |703      |31       |93.46088193456615 |65703               |2119.451612903226           |
# |english         |na                   |ca    |786      |34       |94.19338422391857 |74036               |2177.529411764706           |
# |hindi           |android              |in    |883      |7        |29.831257078142695|26341               |3763.0                      |
# |hindi           |androidtv|firetv     |in    |886      |7        |32.19638826185101 |28526               |4075.1428571428573          |
# |tamil           |android              |in    |900      |7        |28.886666666666667|25998               |3714.0                      |
# |english         |android              |in    |953      |7        |29.094438614900316|27727               |3961.0                      |
# |english         |androidtv|firetv     |in    |972      |7        |30.77469135802469 |29913               |4273.285714285715           |
# |hindi           |ios|web|mweb|jiolyf  |in    |1127     |6        |19.82963620230701 |22348               |3724.6666666666665          |
# |kannada         |na                   |in    |1224     |7        |19.951797385620914|24421               |3488.714285714286           |
# |telugu          |na                   |in    |1234     |7        |19.54132901134522 |24114               |3444.8571428571427          |
# |tamil           |ios|web|mweb|jiolyf  |in    |1255     |7        |19.200796812749005|24097               |3442.4285714285716          |
# |english         |ios|web|mweb|jiolyf  |in    |1376     |7        |18.993459302325583|26135               |3733.5714285714284          |
# |hindi           |appletv              |in    |1434     |7        |18.016039051603904|25835               |3690.714285714286           |
# |english         |appletv              |in    |1620     |7        |17.41604938271605 |28214               |4030.5714285714284          |
# |hindi           |android              |in    |4438     |33       |34.03853086976115 |151063              |4577.666666666667           |
# |hindi           |androidtv|firetv     |in    |4559     |33       |36.00460627330555 |164145              |4974.090909090909           |
# |tamil           |android              |in    |4579     |33       |32.8368639440926  |150360              |4556.363636363636           |
# |english         |android              |in    |5213     |37       |34.510262804527144|179902              |4862.216216216216           |
# |english         |androidtv|firetv     |in    |5319     |37       |35.89734912577552 |190938              |5160.486486486487           |
# |kannada         |na                   |in    |6302     |33       |23.44509679466836 |147751              |4477.30303030303            |
# |tamil           |ios|web|mweb|jiolyf  |in    |6380     |33       |22.15297805642633 |141336              |4282.909090909091           |
# |telugu          |na                   |in    |6454     |33       |22.018593120545397|142108              |4306.30303030303            |
# |hindi           |ios|web|mweb|jiolyf  |in    |6458     |33       |21.92195726231031 |141572              |4290.060606060606           |
# |hindi           |appletv              |in    |7135     |33       |20.962578836720393|149568              |4532.363636363636           |
# |english         |ios|web|mweb|jiolyf  |in    |7650     |37       |22.46535947712418 |171860              |4644.864864864865           |
# |english         |appletv              |in    |8488     |37       |21.39031573986805 |181561              |4907.054054054054           |
# +----------------+---------------------+------+---------+---------+------------------+--------------------+----------------------------+
# playout_df.where('content_language="hindi" and platform="android" and tenant="in"').groupBy('content_id').count().orderBy('content_id').show(200, False)
# playout_df.where('content_id = "1540019017"').groupBy('content_language', 'platform', 'tenant').count().orderBy('count').show(200, False)


def get_break_list(playout_df, filter):
    cols = ['content_id', 'start_time', 'end_time', 'delivered_duration']
    if filter == 1:
        playout_df = playout_df \
            .withColumn('rank', F.expr('row_number() over (partition by content_id order by start_time)')) \
            .withColumn('rank_next', F.expr('rank+1'))
        res_df = playout_df \
            .join(playout_df.selectExpr('content_id', 'rank_next as rank', 'end_time as end_time_next'),
                  ['content_id', 'rank']) \
            .withColumn('bias', F.expr('cast(unix_timestamp(start_time, "HH:mm:ss") as long) - cast(unix_timestamp(end_time_next, "HH:mm:ss") as long)')) \
            .where('bias >= 0') \
            .orderBy('start_time')
        res_df = playout_df \
            .where('rank = 1') \
            .select(*cols) \
            .union(res_df.select(*cols)) \
            .withColumn('simple_start_time', F.expr('substring(start_time, 1, 5)'))
    elif filter == 2:
        res_df = playout_df\
            .where('content_language="hindi" and platform="android" and tenant="in"')\
            .select(*cols)\
            .withColumn('simple_start_time', F.expr('substring(start_time, 1, 5)'))
    elif filter == 3:
        res_df = playout_df\
            .where('content_language="english" and platform="android" and tenant="in"')\
            .select(*cols)\
            .withColumn('simple_start_time', F.expr('substring(start_time, 1, 5)'))
    else:
        res_df = playout_df \
            .where('content_language="english" and platform="androidtv|firetv" and tenant="in"') \
            .select(*cols) \
            .withColumn('simple_start_time', F.expr('substring(start_time, 1, 5)'))
    save_data_frame(res_df, live_ads_inventory_forecasting_root_path + f"/test_dataset/break_start_time_data_{filter}_of_{tournament}")


# data_source = "concurrency"
data_source = "watched_video"
if data_source == "concurrency":
    concurrency_df = reduce(lambda x, y: x.union(y),
                        [load_data_frame(spark, f"{ssai_concurrency_path}/cd={date[0]}").withColumn('date', F.lit(date[0])) for date in valid_dates]) \
        .where('ssai_tag is not null and ssai_tag != "" and no_user > 0')\
        .join(match_df, ['date', 'content_id'])\
        .withColumn('simple_start_time', F.expr('substring(from_utc_timestamp(time, "IST"), 12, 5)'))\
        .withColumn('no_user', F.expr('cast(no_user as float)'))\
        .groupBy('date', 'content_id', 'simple_start_time')\
        .agg(F.sum('no_user').alias('no_user'))\
        .cache()
elif data_source == "watched_video":
    watch_video_df = reduce(lambda x, y: x.union(y),
                        [load_data_frame(spark, f"{watch_video_path}/cd={date[0]}")
                            .withColumn('date', F.lit(date[0]))
                            .select('timestamp', 'received_at', 'watch_time', 'content_id', 'dw_p_id', 'dw_d_id', 'date') for date in valid_dates]) \
        .join(match_df, ['date', 'content_id'])\
        .withColumn('end_timestamp', F.substring(F.col('timestamp'), 1, 19))\
        .withColumn('end_timestamp', F.expr('if(end_timestamp <= received_at, end_timestamp, received_at)'))\
        .withColumn('watch_time', F.expr('cast(watch_time as int)'))\
        .withColumn('start_timestamp', F.from_unixtime(F.unix_timestamp(F.col('end_timestamp'), 'yyyy-MM-dd HH:mm:ss') - F.col('watch_time')))\
        .withColumn('start_timestamp', F.substring(F.from_utc_timestamp(F.col('start_timestamp'), "IST"), 12, 8))\
        .withColumn('end_timestamp', F.substring(F.from_utc_timestamp(F.col('end_timestamp'), "IST"), 12, 8))\
        .withColumn('start_timestamp_int', F.expr('cast(unix_timestamp(start_timestamp, "HH:mm:ss") as long)'))\
        .withColumn('end_timestamp_int', F.expr('cast(unix_timestamp(end_timestamp, "HH:mm:ss") as long)'))\
        .cache()
    # watch_video_df = reduce(lambda x, y: x.union(y),
    #                         [filter_users(load_data_frame(spark, f"{watch_video_path}/cd={date[0]}")
    #                         .withColumn('date', F.lit(date[0]))
    #                         .select('timestamp', 'received_at', 'watch_time', 'content_id', 'dw_p_id', 'dw_d_id',
    #                                 'date')) for date in valid_dates]) \
    #     .join(match_df, ['date', 'content_id']) \
    #     .withColumn('end_timestamp', F.substring(F.col('timestamp'), 1, 19)) \
    #     .withColumn('end_timestamp', F.expr('if(end_timestamp <= received_at, end_timestamp, received_at)')) \
    #     .withColumn('watch_time', F.expr('cast(watch_time as int)')) \
    #     .withColumn('start_timestamp', F.from_unixtime(
    #     F.unix_timestamp(F.col('end_timestamp'), 'yyyy-MM-dd HH:mm:ss') - F.col('watch_time'))) \
    #     .withColumn('start_timestamp', F.substring(F.from_utc_timestamp(F.col('start_timestamp'), "IST"), 12, 8)) \
    #     .withColumn('end_timestamp', F.substring(F.from_utc_timestamp(F.col('end_timestamp'), "IST"), 12, 8)) \
    #     .withColumn('start_timestamp_int', F.expr('cast(unix_timestamp(start_timestamp, "HH:mm:ss") as long)')) \
    #     .withColumn('end_timestamp_int', F.expr('cast(unix_timestamp(end_timestamp, "HH:mm:ss") as long)')) \
    #     .cache()
    # watch_video_df.show(2, False)

filter_list = [1, 2, 3, 4]
for filter in filter_list[3:]:
    print(f"filter={filter}")
    # get_break_list(playout_df, filter)
    final_playout_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/test_dataset/break_start_time_data_{filter}_of_{tournament}") \
        .withColumn('start_time_int', F.expr('cast(unix_timestamp(start_time, "HH:mm:ss") as long)')) \
        .withColumn('end_time_int', F.expr('cast(unix_timestamp(end_time, "HH:mm:ss") as long)')) \
        .cache()
    # final_playout_df.show(2, False)
    # playout_df.where('content_id = "1540019017"').orderBy('start_time').show(200)
    # final_playout_df.where('content_id = "1540019017"').orderBy('start_time').show(200)
    # final_playout_df.orderBy('content_id', 'start_time', 'simple_start_time').show(200)
    # final_playout_df.groupBy('content_id').count().where('count > 1').show()
    # final_playout_df\
    #     .withColumn('delivered_duration_1', F.expr('unix_timestamp(delivered_duration, "hh:mm:ss")'))\
    #     .withColumn('new_delivered_duration', F.expr('cast(unix_timestamp(delivered_duration, "HH:mm:ss") as long)'))\
    #     .show(50, False)
    # for date in valid_dates:
    #     try:
    #         df = load_data_frame(spark, f"{ssai_concurrency_path}/cd={date[0]}").withColumn('date', F.lit(date[0]))
    #     except:
    #         print(date)
    if data_source == "concurrency":
        total_inventory_df = concurrency_df\
            .join(final_playout_df, ['content_id', 'simple_start_time'])\
            .withColumn('inventory', F.expr('delivered_duration / 10 * no_user'))\
            .groupBy('date', 'content_id') \
            .agg(F.count('start_time').alias('break_num'),
                 F.sum('no_user').alias('total_cocurrency'),
                 F.sum('delivered_duration').alias('total_break_duration'),
                 F.sum('inventory').alias('total_inventory')) \
            .cache()
        save_data_frame(total_inventory_df, live_ads_inventory_forecasting_root_path + f"/test_dataset/{data_source}_{filter}_of_{tournament}")
        # total_concurrency_df.orderBy('date', 'content_id').show(200)
    elif data_source == "watched_video":
        total_inventory_df = watch_video_df\
            .join(F.broadcast(final_playout_df), ['content_id'])\
            .where('(start_timestamp < start_time and end_timestamp > start_time) or (start_timestamp >= start_time and start_timestamp < end_time)')\
            .withColumn('valid_duration', F.expr('if(start_timestamp < start_time, '
                                                 'if(end_timestamp < end_time, end_timestamp_int - start_time_int, end_time_int - start_time_int), '
                                                 'if(end_timestamp < end_time, end_timestamp_int - start_timestamp_int, end_time_int - start_timestamp_int))'))\
            .groupBy('date', 'content_id')\
            .agg(F.sum('valid_duration').alias('total_inventory'),
                 F.countDistinct("dw_p_id").alias('total_pid_reach'),
                 F.countDistinct("dw_d_id").alias('total_did_reach'))\
            .withColumn('total_inventory', F.expr('cast(total_inventory / 10 as bigint)'))\
            .cache()
        save_data_frame(total_inventory_df, live_ads_inventory_forecasting_root_path + f"/test_dataset/{data_source}_{filter}_of_{tournament}")


data_source = "watched_video"
cols = ['date', 'content_id', 'title', 'shortsummary']
filter_list = [1, 2, 3, 4]
for col in ['inventory', 'pid_reach', 'did_reach']:
    for filter in filter_list:
        cols.append(f"wt_{col}_{filter}")

match_df.join(reduce(lambda x, y: x.join(y, ['date', 'content_id'], 'left'),
    [load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/test_dataset/{data_source}_{filter}_of_{tournament}")
                     .withColumnRenamed('total_inventory', 'wt_inventory_'+str(filter))
                     .withColumnRenamed('total_pid_reach', 'wt_pid_reach_'+str(filter))
                     .withColumnRenamed('total_did_reach', 'wt_did_reach_'+str(filter))
                     .select('date', 'content_id', 'wt_inventory_'+str(filter), 'wt_pid_reach_'+str(filter), 'wt_did_reach_'+str(filter)) for filter in filter_list]),
              ['date', 'content_id'], 'left')\
    .orderBy('date', 'content_id')\
    .select(*cols)\
    .show(200, False)

filter_list = [1, 2, 3]
res_df = match_df.join(reduce(lambda x, y: x.join(y, ['date', 'content_id'], 'left'),
    [load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/test_dataset/{data_source}_{filter}_of_{tournament}")
                    .withColumnRenamed('total_inventory', 'total_inventory' + str(filter))
                          .withColumnRenamed('total_pid_reach', 'total_pid_reach'+str(filter))
                          .withColumnRenamed('total_did_reach', 'total_did_reach'+str(filter)) for filter in filter_list]),
              ['date', 'content_id'], 'left')\
    .fillna(-1, ['total_inventory1', 'total_pid_reach1', 'total_did_reach1',
                 'total_inventory2', 'total_pid_reach2', 'total_did_reach2',
                 'total_inventory3', 'total_pid_reach3', 'total_did_reach3'])\
    .withColumn('total_inventory', avg_value_udf('total_inventory1', 'total_inventory2', 'total_inventory3'))\
    .withColumn('total_pid_reach', avg_value_udf('total_pid_reach1', 'total_pid_reach2', 'total_pid_reach3'))\
    .withColumn('total_did_reach', avg_value_udf('total_did_reach1', 'total_did_reach2', 'total_did_reach3'))\
    .drop('total_inventory1', 'total_pid_reach1', 'total_did_reach1',
          'total_inventory2', 'total_pid_reach2', 'total_did_reach2',
          'total_inventory3', 'total_pid_reach3', 'total_did_reach3')\
    .orderBy('date', 'content_id')
res_df.show(200, False)
save_data_frame(res_df, live_ads_inventory_forecasting_root_path + f"/final_test_dataset/{data_source}_of_{tournament}")
