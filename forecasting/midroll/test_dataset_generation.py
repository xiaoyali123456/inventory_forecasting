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


def avg_value(*args):
    res = 0
    count = 0
    for v in args:
        if v > 0:
            res += v
            count += 1
    if count == 0:
        return -1
    return int(res/count)


def get_break_list(playout_df, filter):
    cols = ['content_id', 'start_time', 'end_time', 'delivered_duration']
    if filter == 1:
        playout_df = playout_df \
            .withColumn('rank', F.expr('row_number() over (partition by content_id order by start_time)')) \
            .withColumn('rank_next', F.expr('rank+1'))
        res_df = playout_df \
            .join(playout_df.selectExpr('content_id', 'rank_next as rank', 'end_time as end_time_next'),
                  ['content_id', 'rank']) \
            .withColumn('bias', F.expr('cast(unix_timestamp(start_time, "yyyy-MM-dd HH:mm:ss") as long) '
                                       '- cast(unix_timestamp(end_time_next, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .where('bias >= 0') \
            .orderBy('start_time')
        res_df = playout_df \
            .where('rank = 1') \
            .select(*cols) \
            .union(res_df.select(*cols))
    elif filter == 2:
        res_df = playout_df\
            .where('content_language="hindi" and platform="android" and tenant="in"')\
            .select(*cols)
    elif filter == 3:
        res_df = playout_df\
            .where('content_language="english" and platform="android" and tenant="in"')\
            .select(*cols)
    else:
        res_df = playout_df \
            .where('content_language="english" and platform="androidtv|firetv" and tenant="in"') \
            .select(*cols)
    save_data_frame(res_df, live_ads_inventory_forecasting_root_path + f"/test_dataset/break_start_time_data_{filter}_of_{tournament}")


check_title_valid_udf = F.udf(check_title_valid, IntegerType())
avg_value_udf = F.udf(avg_value, LongType())
max_value_udf = F.udf(lambda *args: max(args), LongType())
strip_udf = F.udf(lambda x: x.strip(), StringType())

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
tournament = "ipl2020"
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


def save_playout_data(tournament):
    match_df, valid_dates, complete_valid_dates = get_match_data(tournament)
    if tournament in ["wc2021", "ipl2022", "wc2022"]:
        play_out_input_path = play_out_log_v2_input_path
    elif valid_dates[0][0] >= "2021-09-09":
        play_out_input_path = play_out_log_input_path
    # for inventory calculation from concurrency and playout data
    if valid_dates[0][0] < "2022-03-26":
        playout_df = reduce(lambda x, y: x.union(y),
                            [load_data_frame(spark, f"{play_out_input_path}{date[0]}", 'csv', True)
                            .withColumn('date', F.lit(date[0]))
                            .withColumnRenamed(content_id_col2, 'content_id')
                            .withColumnRenamed(start_time_col2, 'start_time')
                            .withColumnRenamed(end_time_col2, 'end_time')
                            .withColumnRenamed(break_duration_col2, 'delivered_duration')
                            .withColumnRenamed(creative_id_col2, 'creative_id')
                            .withColumnRenamed(break_id_col2, 'break_id')
                            .withColumnRenamed(playout_id_col2, 'playout_id')
                            .withColumnRenamed(creative_path_col2, 'creative_path')
                            .withColumnRenamed(content_id_col, 'content_id')
                            .withColumnRenamed(start_time_col, 'start_time')
                            .withColumnRenamed(end_time_col, 'end_time')
                            .withColumnRenamed(break_duration_col, 'delivered_duration')
                            .withColumnRenamed(creative_id_col, 'creative_id')
                            .withColumnRenamed(break_id_col, 'break_id')
                            .withColumnRenamed(playout_id_col, 'playout_id')
                            .withColumnRenamed(creative_path_col, 'creative_path')
                            .select('date', 'content_id', 'start_time', 'end_time', 'delivered_duration',
                                    'creative_id', 'break_id', 'playout_id', 'creative_path') for date in valid_dates]) \
                            .withColumn('creative_id', F.expr('upper(creative_id)')) \
                            .withColumn('break_id', F.expr('upper(break_id)')) \
                            .withColumn('content_id', F.trim(F.col('content_id')))
    else:
        playout_df = reduce(lambda x, y: x.union(y),
                            [load_data_frame(spark, f"{play_out_input_path}{date[0]}", 'csv', True)
                            .withColumn('date', F.lit(date[0]))
                            .withColumnRenamed(content_id_col2, 'content_id')
                            .withColumnRenamed(start_time_col2, 'start_time')
                            .withColumnRenamed(end_time_col2, 'end_time')
                            .withColumnRenamed(break_duration_col2, 'delivered_duration')
                            .withColumnRenamed(platform_col2, 'platform')
                            .withColumnRenamed(tenant_col2, 'tenant')
                            .withColumnRenamed(content_language_col2, 'content_language')
                            .withColumnRenamed(creative_id_col2, 'creative_id')
                            .withColumnRenamed(break_id_col2, 'break_id')
                            .withColumnRenamed(playout_id_col2, 'playout_id')
                            .withColumnRenamed(creative_path_col2, 'creative_path')
                            .withColumnRenamed(content_id_col, 'content_id')
                            .withColumnRenamed(start_time_col, 'start_time')
                            .withColumnRenamed(end_time_col, 'end_time')
                            .withColumnRenamed(break_duration_col, 'delivered_duration')
                            .withColumnRenamed(platform_col, 'platform')
                            .withColumnRenamed(tenant_col, 'tenant')
                            .withColumnRenamed(content_language_col, 'content_language')
                            .withColumnRenamed(creative_id_col, 'creative_id')
                            .withColumnRenamed(break_id_col, 'break_id')
                            .withColumnRenamed(playout_id_col, 'playout_id')
                            .withColumnRenamed(creative_path_col, 'creative_path')
                            .select('date', 'content_id', 'start_time', 'end_time', 'delivered_duration',
                                    'platform', 'tenant', 'content_language', 'creative_id', 'break_id',
                                    'playout_id', 'creative_path') for date in valid_dates]) \
            .withColumn('content_language', F.expr('lower(content_language)')) \
            .withColumn('platform', F.expr('lower(platform)')) \
            .withColumn('tenant', F.expr('lower(tenant)')) \
            .withColumn('creative_id', F.expr('upper(creative_id)')) \
            .withColumn('break_id', F.expr('upper(break_id)')) \
            .withColumn('content_id', F.trim(F.col('content_id')))
        print('unvalid stream count:')
        print(playout_df.where('content_language is null or platform is null or tenant is null').count())
    # playout_df\
    #     .where('content_id="1540008539"')\
    #     .withColumn('len', F.length('start_time')) \
    #     .withColumn('start_time2', F.expr('if(length(end_time)==8, end_time, from_unixtime(unix_timestamp(end_time, "hh:mm:ss aa"), "HH:mm:ss"))'))\
    #     .where('start_time2 is null').show()
    playout_df = playout_df \
        .withColumn('tournament', F.lit(tournament)) \
        .withColumn('creative_path', F.expr('lower(creative_path)')) \
        .where('content_id != "Content ID" and start_time is not null and end_time is not null') \
        .withColumn('start_time', strip_udf('start_time')) \
        .withColumn('start_time', F.expr('if(length(start_time)==7 and tournament="ac2022", concat_ws("", "0", start_time), start_time)')) \
        .withColumn('start_time', F.expr('if(content_id="1540017117", concat_ws(" ", start_time, "pm"), start_time)')) \
        .withColumn('start_time', F.expr('if(length(start_time)==11 and substring(start_time, 1, 8) >= "13:00:00" and tournament = "ac2022", substring(start_time, 1, 8), start_time)')) \
        .withColumn('start_time', F.expr('if(length(start_time)==8, start_time, from_unixtime(unix_timestamp(start_time, "hh:mm:ss aa"), "HH:mm:ss"))')) \
        .withColumn('end_time', strip_udf('end_time')) \
        .withColumn('end_time', F.expr('if(length(end_time)==7 and tournament="ac2022", concat_ws("", "0", end_time), end_time)')) \
        .withColumn('end_time', F.expr('if(content_id="1540017117", concat_ws(" ", end_time, "pm"), end_time)')) \
        .withColumn('end_time', F.expr('if(length(end_time)==11 and substring(end_time, 1, 8) >= "13:00:00" and tournament = "ac2022", substring(end_time, 1, 8), end_time)')) \
        .withColumn('end_time', F.expr('if(length(end_time)==8, end_time, from_unixtime(unix_timestamp(end_time, "hh:mm:ss aa"), "HH:mm:ss"))')) \
        .withColumn('delivered_duration', F.expr('cast(unix_timestamp(delivered_duration, "HH:mm:ss") as long)'))
    # # .withColumn('date', F.expr('if(content_id="1540014335", "2022-05-27", date)'))\
    # # .withColumn('date', F.expr('if(content_id="1540009355", "2021-11-01", date)'))\
    # playout_df.where('date = "2023-01-05"').orderBy('start_time').show(200, False)
    if playout_df.where('start_time is null or end_time is null').count() == 0:
        playout_df = playout_df\
            .where('content_id != "Content ID" and content_id is not null '
                   'and start_time is not null and end_time is not null')\
            .withColumn('simple_start_time', F.expr('substring(start_time, 1, 5)'))\
            .withColumn('next_date', F.date_add(F.col('date'), 1))\
            .withColumn('start_date', F.expr('if(start_time < "03:00:00", next_date, date)'))\
            .withColumn('end_date', F.expr('if(end_time < "03:00:00", next_date, date)'))\
            .withColumn('start_time', F.concat_ws(" ", F.col('start_date'), F.col('start_time')))\
            .withColumn('end_time', F.concat_ws(" ", F.col('end_date'), F.col('end_time'))) \
            .join(match_df.select('date', 'content_id'), ['date', 'content_id'])\
            .withColumn('start_time_int', F.expr('cast(unix_timestamp(start_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .withColumn('end_time_int', F.expr('cast(unix_timestamp(end_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .withColumn('duration', F.expr('end_time_int-start_time_int'))\
            .where('duration > 0')
        save_data_frame(playout_df, play_out_log_v3_input_path+tournament)
        playout_df \
            .where('creative_path != "aston"')\
            .groupBy('content_id') \
            .agg(F.min('start_time').alias('min_start_time'), F.min('end_time').alias('min_end_time'),
                 F.max('start_time').alias('max_start_time'), F.max('end_time').alias('max_end_time'),
                 F.max('duration')) \
            .withColumn('start_time_int', F.expr('cast(unix_timestamp(min_start_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .withColumn('end_time_int', F.expr('cast(unix_timestamp(max_end_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .withColumn('duration', F.expr('(end_time_int-start_time_int)/3600'))\
            .orderBy('content_id') \
            .show(1000, False)
        playout_df \
            .show(100, False)
        print(f"save {tournament} playout data done!")
    else:
        playout_df.where('start_time is null or end_time is null').groupBy('date', 'content_id').count().show()


def save_wc2019_playout_data(tournament):
    match_df, valid_dates, complete_valid_dates = get_match_data(tournament)
    if tournament in ["wc2021", "ipl2022", "wc2022"]:
        play_out_input_path = play_out_log_v2_input_path
    else:
        play_out_input_path = play_out_log_input_path
    # for inventory calculation from concurrency and playout data
    playout_df = load_data_frame(spark, f"s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/playout_log_v2/{tournament}/")\
        .selectExpr('start_date as date', 'content_id', 'break_ist_start_time as start_time', 'break_ist_date as start_date', 'duration as delivered_duration',
                                'platform', 'tenant', 'language as content_language', 'file_name')\
        .withColumn('tournament', F.lit(tournament))\
        .withColumn('content_language', F.expr('lower(content_language)'))\
        .withColumn('platform', F.expr('lower(platform)'))\
        .withColumn('tenant', F.expr('lower(tenant)'))\
        .withColumn('content_id', F.trim(F.col('content_id'))) \
        .where('start_time is not null')\
        .withColumn('creative_path', F.expr('if(locate("aston", file_name)>0, "aston", if(locate("spot", file_name)>0, "spot", if(locate("ssai", file_name)>0, "ssai", "")))'))
    if playout_df.where('start_time is null').count() == 0:
        playout_df = playout_df\
            .where('content_id != "Content ID" and content_id is not null and start_time is not null')\
            .withColumn('simple_start_time', F.expr('substring(start_time, 1, 5)')) \
            .withColumn('next_date', F.date_add(F.col('date'), 1)) \
            .withColumn('start_time_tmp', F.concat_ws(" ", F.col('start_date'), F.col('start_time'))) \
            .withColumn('date', F.expr('if(content_id="1440000982" and start_time_tmp<="2019-07-10 03:00:00", "2019-07-09", date)')) \
            .withColumn('content_id', F.expr('if(content_id="1440000982" and start_time_tmp<="2019-07-10 03:00:00", 1440000724, content_id)')) \
            .withColumn('start_date', F.expr('if(start_date=next_date and start_time>="23:00:00", date, start_date)'))\
            .drop('start_time_tmp') \
            .withColumn('start_time', F.concat_ws(" ", F.col('start_date'), F.col('start_time'))) \
            .withColumn('end_time', F.from_unixtime(F.unix_timestamp(F.col('start_time'), 'yyyy-MM-dd HH:mm:ss') + F.col('delivered_duration'))) \
            .join(match_df.select('date', 'content_id'), ['date', 'content_id'])\
            .withColumn('start_time_int', F.expr('cast(unix_timestamp(start_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .withColumn('end_time_int', F.expr('cast(unix_timestamp(end_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .withColumn('duration', F.expr('end_time_int-start_time_int'))\
            .where('duration > 0 and duration < 3600')
        save_data_frame(playout_df, play_out_log_v3_input_path+tournament)
        playout_df \
            .groupBy('content_id') \
            .agg(F.min('start_time').alias('min_start_time'), F.min('end_time').alias('min_end_time'),
                 F.max('start_time').alias('max_start_time'), F.max('end_time').alias('max_end_time'),
                 F.count('*').alias('break_num')) \
            .withColumn('start_time_int', F.expr('cast(unix_timestamp(min_start_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .withColumn('end_time_int', F.expr('cast(unix_timestamp(max_end_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .withColumn('match_duration', F.expr('(end_time_int-start_time_int)/3600'))\
            .orderBy('content_id') \
            .show(1000, False)
        playout_df \
            .show(100, False)
        print(f"save {tournament} playout data done!")
    else:
        playout_df.where('start_time is null').show()


def save_playout_data_from_original_logs(tournament):
    match_df, valid_dates, complete_valid_dates = get_match_data(tournament)
    play_out_input_path = play_out_log_original_input_path
    # for inventory calculation from concurrency and playout data
    playout_df = reduce(lambda x, y: x.union(y),
                        [load_data_frame(spark, f"{play_out_input_path}{date[0]}", 'csv', True)
                        .withColumn('tournament', F.lit(tournament))
                        .withColumn('start_date', F.expr('substring(date, 1, 10)'))
                        .withColumnRenamed('date', 'date_original')
                        .withColumn('date', F.lit(date[0]))
                        .withColumn('start_date', F.expr('if(start_date < date, date, start_date)'))
                        .withColumnRenamed('time_in', 'start_time')
                        .withColumn('start_time', strip_udf('start_time'))
                        .withColumn('start_time', F.expr('if(length(start_time)==8, start_time, from_unixtime(unix_timestamp(start_time, "hh:mm:ss aa"), "HH:mm:ss"))'))
                        .withColumnRenamed('duration', 'delivered_duration')
                        .withColumn('creative_path', F.expr('if(locate("aston", file_name)>0, "aston", if(locate("spot", file_name)>0, "spot", if(locate("ssai", file_name)>0, "ssai", "")))'))
                        .where('start_time is not null')
                        .withColumn('start_time', F.concat_ws(" ", F.col('start_date'), F.col('start_time'))) \
                        .withColumn('end_time', F.from_unixtime(F.unix_timestamp(F.col('start_time'), 'yyyy-MM-dd HH:mm:ss') + F.col('delivered_duration')))
                        .select('date', 'start_date', 'start_time', 'end_time', 'delivered_duration', 'creative_path') for date in valid_dates])
    if match_df.join(playout_df.select('date').distinct(), 'date').groupBy('date').count().where('count > 1').count() > 0:
        print('multiple matches in one date')
        match_df.groupBy('date').count().where('count > 1').show()
        # return 0
    else:
        print('only one match in one date!!!')
        playout_df = playout_df\
            .join(match_df.select('date', 'content_id'), 'date')
        print(match_df.count())
        print(playout_df.select('content_id').distinct().count())
    if playout_df.where('start_time is null or end_time is null').count() == 0:
        playout_df = playout_df \
            .where('content_id != "Content ID" and content_id is not null '
                   'and start_time is not null and end_time is not null') \
            .withColumn('start_time_int', F.expr('cast(unix_timestamp(start_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .withColumn('end_time_int', F.expr('cast(unix_timestamp(end_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .withColumn('duration', F.expr('end_time_int-start_time_int')) \
            .where('duration > 0')
        save_data_frame(playout_df, play_out_log_v3_input_path+tournament)
        playout_df \
            .where('creative_path != "aston"')\
            .groupBy('date', 'content_id') \
            .agg(F.min('start_time').alias('min_start_time'), F.min('end_time').alias('min_end_time'),
                 F.max('start_time').alias('max_start_time'), F.max('end_time').alias('max_end_time'),
                 F.max('duration'), F.sum('duration'), F.avg('duration')) \
            .withColumn('start_time_int', F.expr('cast(unix_timestamp(min_start_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .withColumn('end_time_int', F.expr('cast(unix_timestamp(max_end_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .withColumn('duration', F.expr('(end_time_int-start_time_int)/3600'))\
            .orderBy('content_id') \
            .show(1000, False)
        playout_df \
            .show(100, False)
        print(f"save {tournament} playout data done!")
    else:
        playout_df.where('start_time is null or end_time is null').groupBy('date', 'content_id').count().show()


def calculate_break_statistics(playout_df):
    playout_df = playout_df.where('creative_path = "ssaid"')
    res_df = playout_df \
        .withColumn('break_id', F.expr('if(creative_path = "ssaid", creative_id, break_id)'))\
        .groupBy('content_id', 'playout_id', 'break_id')\
        .agg(F.sum('delivered_duration').alias('break_duration')) \
        .groupBy('content_id', 'playout_id') \
        .agg(F.sum('break_duration').alias('total_break_duration'),
             F.count('break_id').alias('break_num')) \
        .withColumn('avg_break_duration', F.expr('total_break_duration/break_num'))\
        .withColumn('rank', F.expr('row_number() over (partition by content_id order by break_num desc)'))
    res_df = res_df\
        .join(res_df.where('rank=1').select('content_id', 'break_num'), ['content_id', 'break_num']) \
        .withColumn('rank', F.expr('row_number() over (partition by content_id order by avg_break_duration desc)'))\
        .where('rank = 1')
    res_df.orderBy('content_id').show(1000)
    res_df.groupBy('rank').avg('break_num', 'avg_break_duration').show(1000)
    res_df = playout_df \
        .groupBy('content_id', 'playout_id') \
        .agg(F.min('start_time').alias('min_start_time'), F.min('end_time').alias('min_end_time'),
            F.max('start_time').alias('max_start_time'), F.max('end_time').alias('max_end_time')) \
        .withColumn('start_time_int', F.expr('cast(unix_timestamp(min_start_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
        .withColumn('end_time_int', F.expr('cast(unix_timestamp(max_end_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
        .withColumn('duration', F.expr('(end_time_int-start_time_int)/60')) \
        .withColumn('rank', F.expr('row_number() over (partition by content_id order by duration desc)')) \
        .where('rank = 1')
    res_df.orderBy('content_id').show(1000)
    res_df.groupBy('rank').avg('duration').show(1000)
    # res_df.groupBy('rank').avg('break_num', 'avg_break_duration').show(1000)
    return res_df


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

output_level = "tournament"
if output_level == "tournament":
    aggr_cols = ["tournament"]
    unit_col = 'content_id'
    output_folder = "/tournament_level"
else:
    aggr_cols = ['date', 'content_id']
    unit_col = 'valid_duration'
    output_folder = ""

print(output_folder)
for tournament in tournament_list:
    # if tournament not in ["wc2019", "south_africa_tour_of_india2022", "ac2022", "wc2022"]:
    #     continue
    print(tournament)
    # if tournament == "wc2019":
    #     save_wc2019_playout_data(tournament)
    # else:
    #     save_playout_data(tournament)
    playout_df = load_data_frame(spark, play_out_log_v3_input_path + tournament) \
        .where('creative_path != "aston"') \
        .cache()
    # calculate_break_statistics(playout_df)
    # continue
    match_df, valid_dates, complete_valid_dates = get_match_data(tournament)
    if valid_dates[0][0] < "2022-03-26":
        filter_list = [1]
    else:
        filter_list = [1, 2, 3]
    if valid_dates[0][0] < "2023-01-01":
        data_source = "watched_video_sampled"
        rate = 4
        wd_path = watch_video_sampled_path
        timestamp_col = "timestamp"
    else:
        data_source = "watched_video"
        rate = 1
        wd_path = watch_video_path
        timestamp_col = "ts_occurred_ms"
    if not check_s3_path_exist(live_ads_inventory_forecasting_root_path+f"/backup/{data_source}_of_{tournament}"):
        if data_source == "concurrency":
            watch_video_df = reduce(lambda x, y: x.union(y),
                                [load_data_frame(spark, f"{ssai_concurrency_path}/cd={date[0]}").withColumn('date', F.lit(date[0])) for date in valid_dates]) \
                .where('ssai_tag is not null and ssai_tag != "" and no_user > 0')\
                .join(match_df, ['date', 'content_id'])\
                .withColumn('simple_start_time', F.expr('substring(from_utc_timestamp(time, "IST"), 12, 5)'))\
                .withColumn('no_user', F.expr('cast(no_user as float)'))\
                .groupBy('date', 'content_id', 'simple_start_time')\
                .agg(F.sum('no_user').alias('no_user'))\
                .cache()
        else:
            # data_source == "watched_video" or data_source == "watched_video_sampled":
            if valid_dates[0][0] < "2023-01-01":
                watch_video_df = reduce(lambda x, y: x.union(y),
                                        [load_data_frame(spark, f"{wd_path}/cd={date}")
                                        .select("timestamp", 'received_at', 'watch_time', 'content_id', 'dw_p_id', 'dw_d_id')
                                         for date in complete_valid_dates])
            else:
                watch_video_df = reduce(lambda x, y: x.union(y),
                                        [load_data_frame(spark, f"{wd_path}/cd={date}")
                                        .withColumn("timestamp",
                                                    F.expr(f'if(timestamp is null and {timestamp_col} is not null, '
                                                           f'from_unixtime({timestamp_col}/1000), timestamp)'))
                                        .select("timestamp", 'received_at', 'watch_time', 'content_id', 'dw_p_id',
                                                'dw_d_id')
                                         for date in complete_valid_dates])
            watch_video_df = watch_video_df\
                .join(match_df, ['content_id']) \
                .withColumn('end_timestamp', F.substring(F.col('timestamp'), 1, 19)) \
                .withColumn('end_timestamp', F.expr('if(end_timestamp <= received_at, end_timestamp, received_at)')) \
                .withColumn('watch_time', F.expr('cast(watch_time as int)')) \
                .withColumn('start_timestamp', F.from_unixtime(
                    F.unix_timestamp(F.col('end_timestamp'), 'yyyy-MM-dd HH:mm:ss') - F.col('watch_time'))) \
                .withColumn('start_timestamp', F.from_utc_timestamp(F.col('start_timestamp'), "IST")) \
                .withColumn('end_timestamp', F.from_utc_timestamp(F.col('end_timestamp'), "IST")) \
                .withColumn('start_timestamp_int',
                            F.expr('cast(unix_timestamp(start_timestamp, "yyyy-MM-dd HH:mm:ss") as long)')) \
                .withColumn('end_timestamp_int', F.expr('cast(unix_timestamp(end_timestamp, "yyyy-MM-dd HH:mm:ss") as long)')) \
                .drop('received_at', 'timestamp', 'start_timestamp', 'end_timestamp')\
                .cache()
        save_data_frame(watch_video_df, live_ads_inventory_forecasting_root_path+f"/backup/{data_source}_of_{tournament}")
        print(f"save watch video for {tournament} done!")
    else:
        watch_video_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path+f"/backup/{data_source}_of_{tournament}")\
            .withColumn('tournament', F.lit(tournament)) \
            .cache()
        print(f"load watch video for {tournament} done!")
    # watch_video_df\
    #     .groupBy('content_id')\
    #     .agg(F.countDistinct("dw_p_id").alias('total_pid_reach'),
    #         F.countDistinct("dw_d_id").alias('total_did_reach'), F.sum('watch_time'))\
    #     .orderBy('content_id')\
    #     .show(1000, False)
    for filter in filter_list:
        print(f"filter={filter}")
        # get_break_list(playout_df, filter)
        final_playout_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/test_dataset/break_start_time_data_{filter}_of_{tournament}") \
            .withColumn('start_time_int', F.expr('cast(unix_timestamp(start_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .withColumn('end_time_int', F.expr('cast(unix_timestamp(end_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .withColumn('duration', F.expr('end_time_int-start_time_int'))\
            .where('duration > 0 and duration < 3600')\
            .cache()
        # final_playout_df.where('content_id="1540009340"').orderBy('start_time_int').show(1000, False)
        print(final_playout_df.count())
        print(final_playout_df.select('content_id').join(match_df, ['content_id']).select('content_id').distinct().count())
        if data_source == "concurrency":
            total_inventory_df = watch_video_df\
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
        elif data_source == "watched_video" or data_source == "watched_video_sampled":
            total_inventory_df = watch_video_df\
                .join(F.broadcast(final_playout_df), ['content_id']) \
                .where('content_id not in ("1440000689", "1440000694", "1440000696", "1440000982", '
                       '"1540019005", "1540019014", "1540019017","1540016333")') \
                .where('date != "2022-08-24"') \
                .where('(start_timestamp_int < start_time_int and end_timestamp_int > start_time_int) or (start_timestamp_int >= start_time_int and start_timestamp_int < end_time_int)')\
                .withColumn('valid_duration', F.expr('if(start_timestamp_int < start_time_int, '
                                                     'if(end_timestamp_int < end_time_int, end_timestamp_int - start_time_int, end_time_int - start_time_int), '
                                                     'if(end_timestamp_int < end_time_int, end_timestamp_int - start_timestamp_int, end_time_int - start_timestamp_int))'))\
                .withColumn('valid_duration', F.expr('cast(valid_duration as bigint)'))\
                .groupBy(aggr_cols)\
                .agg(F.sum('valid_duration').alias('total_duration'),
                     F.countDistinct(unit_col).alias('match_num'),
                     F.countDistinct("dw_p_id").alias('total_pid_reach'),
                     F.countDistinct("dw_d_id").alias('total_did_reach'))\
                .withColumn('total_inventory', F.expr(f'cast((total_duration / 10 * {rate}) as bigint)')) \
                .withColumn('total_pid_reach', F.expr(f'cast((total_pid_reach * {rate}) as bigint)'))\
                .withColumn('total_did_reach', F.expr(f'cast((total_did_reach * {rate}) as bigint)'))\
                .cache()
            save_data_frame(total_inventory_df, live_ads_inventory_forecasting_root_path + output_folder + f"/test_dataset/{data_source}_{filter}_of_{tournament}")
            # total_inventory_df.where('total_inventory < 0').show()
            total_inventory_df.orderBy(aggr_cols).show()
    col_list = []
    inventory_cols = ['total_inventory'+str(filter) for filter in filter_list]
    pid_reach_cols = ['total_pid_reach'+str(filter) for filter in filter_list]
    did_reach_cols = ['total_did_reach'+str(filter) for filter in filter_list]
    match_num_cols = ['match_num'+str(filter) for filter in filter_list]
    for filter in filter_list:
        col_list.append('total_inventory'+str(filter))
        col_list.append('match_num'+str(filter))
        col_list.append('total_pid_reach'+str(filter))
        col_list.append('total_did_reach'+str(filter))
    if output_level == "tournament":
        res_df = reduce(lambda x, y: x.join(y, aggr_cols, 'left'),
            [load_data_frame(spark, live_ads_inventory_forecasting_root_path + output_folder + f"/test_dataset/{data_source}_{filter}_of_{tournament}")
                            .withColumnRenamed('total_inventory', 'total_inventory'+str(filter))
                            .withColumnRenamed('match_num', 'match_num'+str(filter))
                            .withColumnRenamed('total_pid_reach', 'total_pid_reach'+str(filter))
                            .withColumnRenamed('total_did_reach', 'total_did_reach'+str(filter)).drop('total_duration') for filter in filter_list])\
            .fillna(-1, col_list)\
            .withColumn('total_inventory', max_value_udf(*inventory_cols))\
            .withColumn('match_num', max_value_udf(*match_num_cols))\
            .withColumn('total_pid_reach', max_value_udf(*pid_reach_cols))\
            .withColumn('total_did_reach', max_value_udf(*did_reach_cols))\
            .drop(*col_list)
    else:
        res_df = match_df.join(reduce(lambda x, y: x.join(y, aggr_cols, 'left'),
                                      [load_data_frame(spark,
                                                       live_ads_inventory_forecasting_root_path + output_folder + f"/test_dataset/{data_source}_{filter}_of_{tournament}")
                                      .withColumnRenamed('total_inventory', 'total_inventory' + str(filter))
                                      .withColumnRenamed('total_pid_reach', 'total_pid_reach' + str(filter))
                                      .withColumnRenamed('total_did_reach', 'total_did_reach' + str(filter)).drop(
                                          'total_duration') for filter in filter_list]),
                               aggr_cols, 'left') \
            .fillna(-1, col_list) \
            .withColumn('total_inventory', avg_value_udf(*inventory_cols)) \
            .withColumn('total_pid_reach', avg_value_udf(*pid_reach_cols)) \
            .withColumn('total_did_reach', avg_value_udf(*did_reach_cols)) \
            .drop(*col_list) \
            .orderBy('date', 'content_id')
    # res_df.show(200, False)
    save_data_frame(res_df, live_ads_inventory_forecasting_root_path + output_folder + f"/final_test_dataset/{data_source}_of_{tournament}")
    # res_df.groupBy('shortsummary').sum('total_inventory').show(200, False)
    res_df.show(200, False)
    spark.catalog.clearCache()

