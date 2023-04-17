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
from difflib import get_close_matches

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


import pandas as pd


def reformat_files(root_path, source_format):
    for root, dirs, files in os.walk(root_path):
        for file in files:
            # print(os.path.join(root, file))
            if file.endswith(f".{source_format}"):
                print(os.path.join(root, file))
                # os.rename(os.path.join(root, file), os.path.join(root, file).replace(source_format, target_format))
                # csv_from_excel(os.path.join(root, file), os.path.join(root, file).replace(source_format, target_format))
                df = pd.DataFrame(pd.read_excel(os.path.join(root, file)))
                target_path = "/Users/bingyanghuang/Downloads/ICC_CWC_2019_Log/csv_files/" + file.split("/")[-1].split(".")[0].replace(" ", "")
                df.to_csv(target_path)
    return 0


def date_str(input):
    return input[:4] + "-" + input[4:6] + "-" + input[6:]


def simple_title(title):
    if title.find(": ") > -1:
        title = title.split(": ")[-1]
    teams = sorted(title.split(" vs "))
    return teams[0] + " vs " + teams[1]

import nltk
def correct_language(input):
    word_list = ['hindi', 'english', 'bengali', 'dugout', 'tamil', 'telugu', 'malayalam', 'kannada', 'marathi']
    res = ""
    similarity = 100000
    for word in word_list:
        s = nltk.edit_distance(input, word)
        if s < similarity:
            res = word
            similarity = s
    return res


check_title_valid_udf = F.udf(check_title_valid, IntegerType())
date_str_udf = F.udf(date_str, StringType())
simple_title_udf = F.udf(simple_title, StringType())
correct_language_udf = F.udf(correct_language, StringType())

concurrency_root_path = "s3://hotstar-dp-datalake-processed-us-east-1-prod/hive_internal_database/concurrency.db/"
ssai_concurrency_path = f"{concurrency_root_path}/users_by_live_sports_content_by_ssai"
match_meta_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/ads_crash/match_meta"
live_ads_inventory_forecasting_root_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting"
play_out_log_input_path = "s3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test"


# root_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/playout_logs/wc2019/"
# count = 0
# file_list = []
# for root, dirs, files in os.walk("csv_files"):
#     for file in files:
#         count += 1
#         file_name = file.split("/")[-1]
#         file_list.append(file_name)
#
# print(count)
# df = reduce(lambda x, y: x.union(y),
#     [load_data_frame(spark, f"{root_path}/{file_name}", 'csv', False)
#             .selectExpr('_c0', '_c1 as id1', '_c2 as date', '_c3 as time', '_c4 as file_name',
#                         '_c5 as id2', '_c6 as date_ist', '_c7 as time_ist', '_c8 as duration')
#             .withColumn('_c0', F.expr('cast(_c0 as int)'))
#             .where('_c0 >= 2')
#             .withColumn('match_info', F.lit(file_name)) for file_name in file_list])
# save_data_frame(df, "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/break_info/wc2019")
# tournament = "wc2022"
# tournament = "ipl2022"
# tournament = "wc2021"
# tournament = "ipl2021"
tournament = "wc2019"

match_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"match_data/{tournament}")\
    .withColumn('rank', F.expr('row_number() over (partition by date order by content_id)'))\
    .cache()


df = load_data_frame(spark, "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/break_info/wc2019")\
    .withColumn('file_name', F.expr('lower(file_name)'))\
    .withColumn('match_info', F.expr('lower(match_info)'))\
    .withColumn('time_ist', F.expr('lower(time_ist)'))\
    .withColumn('time_ist_length', F.length(F.col('time_ist')))\
    .withColumn('duration_length', F.length(F.col('duration')))\
    .withColumn('duration_final', F.expr('if(duration_length < 3, cast(duration as int), unix_timestamp(duration, "HH:mm:ss"))'))\
    .withColumn('match_info_list', F.split(F.col('match_info'), "_"))\
    .withColumn('match_info_size', F.size(F.col('match_info_list')))\
    .withColumn('teams_str', F.element_at(F.col('match_info_list'), 4))\
    .withColumn('teams_str', F.regexp_replace(F.col('teams_str'), "vs", " vs "))\
    .withColumn('teams_str', simple_title_udf('teams_str'))\
    .withColumn('date_tmp', F.element_at(F.col('match_info_list'), 1))\
    .withColumn('date_tmp', date_str_udf('date_tmp'))\
    .withColumn('language', F.element_at(F.col('match_info_list'), 5))\
    .withColumn('new_language', correct_language_udf('language'))\
    .withColumn('language_final', F.expr('if(language != "ios", new_language, "")'))\
    .withColumn('platform', F.element_at(F.col('match_info_list'), 6))\
    .withColumn('platform', F.expr('if(match_info_size = 6, null, platform)')) \
    .withColumn('platform_final', F.expr('if(platform = "ios", "ios", "")')) \
    .withColumn('tenant_final', F.expr('if(platform in ("us", "india", "canada"), platform, "")')) \
    .withColumn('date_ist_final', F.substring(F.col('date'), 1, 10)) \
    .withColumn('date_ist_final', F.expr('if(date_ist_final < "2019-05-30" or date_ist_final > "2019-07-15", date_tmp, date_ist_final)')) \
    .where('time_ist_length >= 8 and time_ist_length <= 15') \
    .withColumn('time_ist_final', F.regexp_replace(F.col('time_ist'), " ", ""))\
    .withColumn('time_ist_final', F.regexp_replace(F.col('time_ist_final'), "am", " AM"))\
    .withColumn('time_ist_final', F.regexp_replace(F.col('time_ist_final'), "pm", " PM"))\
    .withColumn('time_ist_final', F.expr('if(length(time_ist_final)==8, time_ist_final, '
                                         'from_unixtime(unix_timestamp(time_ist_final, "hh:mm:ss aa"), "HH:mm:ss"))'))\
    .cache()


df3 = df\
    .where('time_ist_final > "15:00:00" and date_ist_final = date_tmp')\
    .groupBy('teams_str', 'date_tmp')\
    .agg(F.min('time_ist_final').alias('time_ist_final'))\
    .withColumn('rank', F.expr('row_number() over (partition by date_tmp order by time_ist_final)'))\
    .drop('date')\
    .withColumnRenamed('date_tmp', 'date')\
    .join(match_df, ['date', 'rank'], 'left')\
    .cache()

print(df.count())
res_df = df\
    .selectExpr('_c0 as break_id', 'date_tmp as start_date', 'language_final as language',
                'platform_final as platform', 'tenant_final as tenant',
                'date_ist_final as break_ist_date', 'time_ist_final as break_ist_start_time',
                'duration_final as duration', 'teams_str', 'match_info', "file_name")\
    .join(df3.selectExpr('date as start_date', 'teams_str', 'content_id', 'title', 'shortsummary'),
          ['start_date', 'teams_str'])\
    .withColumn('next_date', F.date_add(F.col('start_date'), 1))\
    .withColumn('break_ist_date', F.expr('if(break_ist_start_time<"02:00:00" and (content_id="1440000715" or content_id="1440000784"), '
                                         'next_date, break_ist_date)'))\
    .drop('teams_str', 'next_date')\
    .cache()
print(res_df.count())
save_data_frame(res_df,
                "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/playout_log_tmp/wc2019/")


load_data_frame(spark, "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/playout_log_v2/wc2019/")\
    .where('content_id="1440000982"').orderBy('break_ist_date', 'break_ist_start_time').show(200, False)

load_data_frame(spark, "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/break_info/wc2019")\
    .withColumn('file_name', F.expr('lower(file_name)'))\
    .withColumn('aston_idx', F.locate("aston", F.col('file_name')))\
    .where('aston_idx > 0').show()


# +---+---+-------------------+---------------+---------------------------------------------------+------------------------+-------------------+--------+--------+-------------------------------------------+
# |_c0|id1|date               |time           |file_name                                          |id2                     |date_ist           |time_ist|duration|match_info                                 |
# +---+---+-------------------+---------------+---------------------------------------------------+------------------------+-------------------+--------+--------+-------------------------------------------+
# |2  |499|2019-06-20 00:00:00|13:10:00.620000|SpotAdv:- D:\Fillers\WC_2019_M25_NZvsSA_HLTS.mov   |WC_2019_M25_NZvsSA_HLTS |2019-06-20 00:00:00|13:10:00|00:23:53|20190620_ICC_CWC_AUSvsBAN_English_IOS_AsRun|
# |3  |500|2019-06-20 00:00:00|13:30:03.720000|Live                                               |null                    |null               |null    |null    |20190620_ICC_CWC_AUSvsBAN_English_IOS_AsRun|
# |4  |502|2019-06-20 00:00:00|13:30:03.730000|Live                                               |null                    |null               |null    |null    |20190620_ICC_CWC_AUSvsBAN_English_IOS_AsRun|
# |5  |503|2019-06-20 00:00:00|13:43:03.270000|SpotAdv:- D:\English\CRWC1910336.mov               |CRWC1910336             |2019-06-20 00:00:00|13:43:03|00:00:20|20190620_ICC_CWC_AUSvsBAN_English_IOS_AsRun|
# |6  |505|2019-06-20 00:00:00|13:43:23.850000|SpotAdv:- D:\English\CRWC1910814.mov               |CRWC1910814             |2019-06-20 00:00:00|13:43:23|00:00:30|20190620_ICC_CWC_AUSvsBAN_English_IOS_AsRun|
# |7  |507|2019-06-20 00:00:00|13:43:54.420000|SpotAdv:- D:\English\CRWC1910474.mov               |CRWC1910474             |2019-06-20 00:00:00|13:43:54|00:00:15|20190620_ICC_CWC_AUSvsBAN_English_IOS_AsRun|
# |8  |509|2019-06-20 00:00:00|13:44:10.050000|SpotAdv:- D:\English\CRWC1910497.mov               |CRWC1910497             |2019-06-20 00:00:00|13:44:10|00:00:20|20190620_ICC_CWC_AUSvsBAN_English_IOS_AsRun|
# |9  |511|2019-06-20 00:00:00|13:44:30.620000|SpotAdv:- D:\English\CRWC1910814.mov               |CRWC1910814             |2019-06-20 00:00:00|13:44:30|00:00:30|20190620_ICC_CWC_AUSvsBAN_English_IOS_AsRun|
# |10 |513|2019-06-20 00:00:00|13:45:01.220000|SpotAdv:- D:\English\CRWC1910337.mov               |CRWC1910337             |2019-06-20 00:00:00|13:45:01|00:00:20|20190620_ICC_CWC_AUSvsBAN_English_IOS_AsRun|
# |11 |515|2019-06-20 00:00:00|13:45:21.810000|SpotAdv:- D:\English\CRWC1910176.mov               |CRWC1910176             |2019-06-20 00:00:00|13:45:21|00:00:20|20190620_ICC_CWC_AUSvsBAN_English_IOS_AsRun|
# |12 |517|2019-06-20 00:00:00|13:45:42.380000|SpotAdv:- D:\English\PromoWC19786.mov              |PromoWC19786            |2019-06-20 00:00:00|13:45:42|00:00:30|20190620_ICC_CWC_AUSvsBAN_English_IOS_AsRun|
# |13 |518|2019-06-20 00:00:00|13:45:51.120000|Live                                               |null                    |null               |null    |null    |20190620_ICC_CWC_AUSvsBAN_English_IOS_AsRun|
# |14 |520|2019-06-20 00:00:00|13:45:51.130000|Live                                               |null                    |null               |null    |null    |20190620_ICC_CWC_AUSvsBAN_English_IOS_AsRun|
# |15 |521|2019-06-20 00:00:00|13:55:45.380000|SpotAdv:- D:\English\CRWC1910336.mov               |CRWC1910336             |2019-06-20 00:00:00|13:55:45|00:00:20|20190620_ICC_CWC_AUSvsBAN_English_IOS_AsRun|
# |16 |523|2019-06-20 00:00:00|13:56:05.990000|SpotAdv:- D:\English\CRWC1910814.mov               |CRWC1910814             |2019-06-20 00:00:00|13:56:05|00:00:30|20190620_ICC_CWC_AUSvsBAN_English_IOS_AsRun|
# |17 |525|2019-06-20 00:00:00|13:56:36.570000|SpotAdv:- D:\English\CRWC1910474.mov               |CRWC1910474             |2019-06-20 00:00:00|13:56:36|00:00:15|20190620_ICC_CWC_AUSvsBAN_English_IOS_AsRun|
# |18 |527|2019-06-20 00:00:00|13:56:52.210000|SpotAdv:- D:\English\CRWC1910497.mov               |CRWC1910497             |2019-06-20 00:00:00|13:56:52|00:00:20|20190620_ICC_CWC_AUSvsBAN_English_IOS_AsRun|
# |19 |529|2019-06-20 00:00:00|13:57:12.770000|SpotAdv:- D:\English\CRWC1910814.mov               |CRWC1910814             |2019-06-20 00:00:00|13:57:12|00:00:30|20190620_ICC_CWC_AUSvsBAN_English_IOS_AsRun|
# |20 |531|2019-06-20 00:00:00|13:57:43.330000|SpotAdv:- D:\English\CRWC1910337.mov               |CRWC1910337             |2019-06-20 00:00:00|13:57:43|00:00:20|20190620_ICC_CWC_AUSvsBAN_English_IOS_AsRun|
# |21 |533|2019-06-20 00:00:00|13:58:03.900000|SpotAdv:- D:\English\CRWC1910176.mov               |CRWC1910176             |2019-06-20 00:00:00|13:58:03|00:00:20|20190620_ICC_CWC_AUSvsBAN_English_IOS_AsRun|
# +---+---+-------------------+---------------+---------------------------------------------------+------------------------+-------------------+--------+--------+-------------------------------------------+
