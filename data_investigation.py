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


check_title_valid_udf = F.udf(check_title_valid, IntegerType())


concurrency_root_path = "s3://hotstar-dp-datalake-processed-us-east-1-prod/hive_internal_database/concurrency.db/"
ssai_concurrency_path = f"{concurrency_root_path}/users_by_live_sports_content_by_ssai"
match_meta_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/ads_crash/match_meta"
live_ads_inventory_forecasting_root_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting"
play_out_log_input_path = "s3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test"

# spark.stop()
# spark = hive_spark('statistics')
# match_df = load_hive_table(spark, "in_cms.match_update_s3")
# save_data_frame(match_df, match_meta_path)
tournament_dic = {"wc2022": "ICC Men\'s T20 World Cup 2022",
                  "ipl2022": "TATA IPL 2022",
                  "wc2021": "ICC Men\'s T20 World Cup 2021",
                  "ipl2021": "VIVO IPL 2021"}
# tournament = "wc2022"
# tournament = "ipl2022"
# tournament = "wc2021"
tournament = "ipl2021"
segment = "content_language"
# segment = "none_content_language"
tournament_list = ["wc2022", "wc2021", "ipl2022", "ipl2021"]
if tournament == "wc2022":
    content_id_col = "Content ID"
    start_time_col = "Start Time"
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

# playout_df = reduce(lambda x, y: x.union(y),
#                     [load_data_frame(spark, f"{play_out_log_input_path}/{date[0]}", 'csv', True).select(content_id_col, start_time_col).withColumn('date', F.lit(date[0])) for date in valid_dates]) \
#     .withColumnRenamed(content_id_col, 'content_id')\
#     .withColumn('content_id', F.trim(F.col('content_id'))) \
#     .withColumnRenamed(start_time_col, 'start_time')\
#     .withColumn('start_time', F.expr('if(length(start_time)==8, start_time, from_unixtime(unix_timestamp(start_time, "hh:mm:ss aa"), "HH:mm:ss"))')) \
#     .where('content_id != "Content ID" and content_id is not null and start_time is not null')\
#     .select('date', 'start_time', 'content_id')\
#     .withColumn('rank', F.expr('row_number() over (partition by content_id order by start_time)'))\
#     .withColumn('rank_next', F.expr('rank-1'))\
#     .cache()
#
# playout_df.show(5, False)
#
# final_playout_df = playout_df\
#     .join(playout_df.selectExpr('content_id', 'rank_next as rank', 'start_time as start_time_next'), ['content_id', 'rank'])\
#     .withColumn('bias', F.expr('cast(unix_timestamp(start_time_next, "HH:mm:ss") as long) - cast(unix_timestamp(start_time, "HH:mm:ss") as long)'))\
#     .where('bias > 60')\
#     .orderBy('start_time')
#
# final_playout_df = playout_df\
#     .where('rank = 1')\
#     .select('content_id', 'start_time')\
#     .union(final_playout_df.select('content_id', 'start_time'))\
#     .withColumn('start_time', F.expr('substring(start_time, 1, 5)'))
#
# save_data_frame(final_playout_df, live_ads_inventory_forecasting_root_path + f"/break_start_time_data_of_{tournament}")
final_playout_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/break_start_time_data_of_{tournament}").cache()

# for date in valid_dates:
#     try:
#         df = load_data_frame(spark, f"{ssai_concurrency_path}/cd={date[0]}").withColumn('date', F.lit(date[0]))
#     except:
#         print(date)


concurrency_df = reduce(lambda x, y: x.union(y),
                    [load_data_frame(spark, f"{ssai_concurrency_path}/cd={date[0]}").withColumn('date', F.lit(date[0])) for date in valid_dates]) \
    .where('ssai_tag is not null and ssai_tag != "" and no_user > 0')\
    .join(match_df, ['date', 'content_id'])\
    .withColumn('start_time', F.expr('substring(from_utc_timestamp(time, "IST"), 12, 5)'))\
    .join(final_playout_df, ['content_id', 'start_time'])\
    .withColumn('no_user', F.expr('cast(no_user as float)'))\
    .withColumn('ssai_tag', F.expr('substring(ssai_tag, 7)'))\
    .withColumn('segment_list', F.split(F.col('ssai_tag'), ':'))\
    .withColumn('segment_num', F.size(F.col('segment_list')))\
    .withColumn('content_language', F.expr('lower(content_language)'))

if segment == "content_language":
    concurrency_df = concurrency_df\
        .select('date', 'content_id', 'content_language', 'no_user')\
        .cache()
    total_concurrency_df = concurrency_df \
        .groupBy('date', 'content_id') \
        .agg(F.sum('no_user').alias('total_concurrency')) \
        .cache()
    res_df = concurrency_df \
        .groupBy('date', 'content_id', 'content_language') \
        .agg(F.sum('no_user').alias('concurrency')) \
        .join(total_concurrency_df, ['date', 'content_id']) \
        .withColumn('concurrency_rate', F.expr('concurrency/total_concurrency')) \
        .orderBy('content_id', 'content_language')
    save_data_frame(res_df, f"{segment}_inventory_distribution_of_{tournament}.csv", "csv", True)
    res_df.show(100, False)
else:
    concurrency_df = concurrency_df\
        .select('date', 'content_id', 'content_language', 'no_user', 'segment_num', F.explode('segment_list').alias('segment'))\
        .withColumn('segment_items', F.split(F.col('segment'), "_"))\
        .withColumn('segment_name', F.element_at(F.col('segment_items'), 1))\
        .withColumn('segment_value', F.element_at(F.col('segment_items'), 2))\
        .cache()
    total_concurrency_df = concurrency_df\
        .groupBy('date', 'content_id', 'segment_name')\
        .agg(F.sum('no_user').alias('total_concurrency'))\
        .cache()
    res_df = concurrency_df \
        .groupBy('date', 'content_id', 'segment_name', 'segment_value')\
        .agg(F.sum('no_user').alias('concurrency'))\
        .join(total_concurrency_df, ['date', 'content_id', 'segment_name'])\
        .withColumn('concurrency_rate', F.expr('concurrency/total_concurrency'))\
        .orderBy('content_id', 'segment_name', 'segment_value')
    res_df = res_df\
        .join(res_df.groupBy('segment_name').agg(F.countDistinct('segment_value').alias('segment_value_num')).where('segment_value_num > 1').select('segment_name'), 'segment_name')
    save_data_frame(res_df, f"{segment}_inventory_distribution_of_{tournament}.csv", "csv", True)
    res_df.show(100, False)


def get_segment_meaning(segment_name):
    global segment_name_dic
    if segment_name in segment_name_dic:
        return segment_name_dic[segment_name]
    else:
        return "Custom cohort"


# get_segment_meaning_udf = F.udf(get_segment_meaning, StringType())
# segment_name_dic = {"A": "Age", "G": "Gender", "S": "State", "M": "Metro", "N": "Network", "P": "Platform", "NCCS": "Affluence", "D": "Device price"}
# cols = ['date', 'content_id', 'segment_name', 'segment_value', 'concurrency', 'total_concurrency', 'concurrency_rate']
# df = reduce(lambda x, y: x.union(y),
#                     [load_data_frame(spark, f"segment_inventory_distribution_of_{tournament_name}.csv", "csv", True)
#             .select(*cols)
#             .withColumn('tournament_name', F.lit(tournament_name)) for tournament_name in tournament_list])\
#     .withColumn('segment_meaning', get_segment_meaning_udf('segment_name'))
# save_data_frame(df, live_ads_inventory_forecasting_root_path + f"/segment_distribution_results")
# res_df = df\
#     .select('tournament_name', 'segment_meaning')\
#     .distinct()\
#     .groupBy('tournament_name')\
#     .agg(F.collect_list('segment_meaning').alias('segment_list'))\
#     .withColumn('segment_list', F.sort_array(F.col('segment_list')))
#
# res_df.show(20, False)
#




