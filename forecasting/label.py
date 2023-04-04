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


def get_break_list(playout_df, filter, tournament):
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
    save_data_frame(res_df, pipeline_base_path + f"/label/break_info/start_time_data_{filter}_of_{tournament}")


def save_playout_data(spark, date, content_id, tournament):
    playout_df = load_data_frame(spark, f"{play_out_log_input_path}{date}", 'csv', True)\
        .withColumn('date', F.lit(date))\
        .withColumnRenamed(content_id_col2, 'content_id')\
        .withColumnRenamed(start_time_col2, 'start_time')\
        .withColumnRenamed(end_time_col2, 'end_time')\
        .withColumnRenamed(break_duration_col2, 'delivered_duration')\
        .withColumnRenamed(platform_col2, 'platform')\
        .withColumnRenamed(tenant_col2, 'tenant')\
        .withColumnRenamed(content_language_col2, 'content_language')\
        .withColumnRenamed(creative_id_col2, 'creative_id')\
        .withColumnRenamed(break_id_col2, 'break_id')\
        .withColumnRenamed(playout_id_col2, 'playout_id')\
        .withColumnRenamed(creative_path_col2, 'creative_path')\
        .withColumnRenamed(content_id_col, 'content_id')\
        .withColumnRenamed(start_time_col, 'start_time')\
        .withColumnRenamed(end_time_col, 'end_time')\
        .withColumnRenamed(break_duration_col, 'delivered_duration')\
        .withColumnRenamed(platform_col, 'platform')\
        .withColumnRenamed(tenant_col, 'tenant')\
        .withColumnRenamed(content_language_col, 'content_language')\
        .withColumnRenamed(creative_id_col, 'creative_id')\
        .withColumnRenamed(break_id_col, 'break_id')\
        .withColumnRenamed(playout_id_col, 'playout_id')\
        .withColumnRenamed(creative_path_col, 'creative_path')\
        .select('date', 'content_id', 'start_time', 'end_time', 'delivered_duration',
                'platform', 'tenant', 'content_language', 'creative_id', 'break_id',
                'playout_id', 'creative_path') \
        .withColumn('content_language', F.expr('lower(content_language)')) \
        .withColumn('platform', F.expr('lower(platform)')) \
        .withColumn('tenant', F.expr('lower(tenant)')) \
        .withColumn('creative_id', F.expr('upper(creative_id)')) \
        .withColumn('break_id', F.expr('upper(break_id)')) \
        .withColumn('content_id', F.trim(F.col('content_id')))\
        .where(f'content_id = "{content_id}"')
    print('unvalid stream count:')
    print(playout_df.where('content_language is null or platform is null or tenant is null').count())
    playout_df = playout_df \
        .withColumn('creative_path', F.expr('lower(creative_path)')) \
        .where('content_id != "Content ID" and start_time is not null and end_time is not null') \
        .withColumn('start_time', strip_udf('start_time')) \
        .withColumn('start_time', F.expr('if(length(start_time)==8, start_time, from_unixtime(unix_timestamp(start_time, "hh:mm:ss aa"), "HH:mm:ss"))')) \
        .withColumn('end_time', strip_udf('end_time')) \
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
            .withColumn('start_time_int', F.expr('cast(unix_timestamp(start_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .withColumn('end_time_int', F.expr('cast(unix_timestamp(end_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .withColumn('duration', F.expr('end_time_int-start_time_int'))\
            .where('duration > 0')
        save_data_frame(playout_df, pipeline_base_path + '/label' + playout_log_path_suffix + f"/tournament={tournament}/contentid={content_id}")
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


def main(spark, date, content_id, tournament_name):
    tournament = tournament_name.replace(" ", "_").lower()
    print(tournament)
    save_playout_data(spark, date, content_id, tournament)
    playout_df = load_data_frame(spark, pipeline_base_path + playout_log_path_suffix + f"/tournament={tournament}/contentid={content_id}") \
        .where('creative_path != "aston"') \
        .cache()
    data_source = "watched_video"
    rate = 1
    wd_path = watch_video_path
    timestamp_col = "ts_occurred_ms"
    if not check_s3_path_exist(live_ads_inventory_forecasting_root_path+f"/label/{data_source}/tournament={tournament}/contentid={content_id}"):
        watch_video_df = load_data_frame(spark, f"{wd_path}/cd={date}")\
            .withColumn("timestamp", F.expr(f'if(timestamp is null and {timestamp_col} is not null, '
                               f'from_unixtime({timestamp_col}/1000), timestamp)'))\
            .select("timestamp", 'received_at', 'watch_time', 'content_id', 'dw_p_id',
                    'dw_d_id')\
            .where(f'content_id = "{content_id}"')\
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
        save_data_frame(watch_video_df, pipeline_base_path+f"/label/{data_source}/tournament={tournament}/contentid={content_id}")
        print(f"save watch video for {tournament} done!")
    else:
        watch_video_df = load_data_frame(spark, pipeline_base_path+f"/label/{data_source}/tournament={tournament}/contentid={content_id}")\
            .cache()
        print(f"load watch video for {tournament} done!")
    filter_list = [1, 2, 3]
    for filter in filter_list:
        print(f"filter={filter}")
        get_break_list(playout_df, filter, tournament)
        final_playout_df = load_data_frame(spark, pipeline_base_path + f"/label/break_info/start_time_data_{filter}_of_{tournament}") \
            .withColumn('start_time_int', F.expr('cast(unix_timestamp(start_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .withColumn('end_time_int', F.expr('cast(unix_timestamp(end_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .withColumn('duration', F.expr('end_time_int-start_time_int'))\
            .where('duration > 0 and duration < 3600')\
            .cache()
        # final_playout_df.where('content_id="1540009340"').orderBy('start_time_int').show(1000, False)
        print(final_playout_df.count())
        total_inventory_df = watch_video_df\
            .join(F.broadcast(final_playout_df), ['content_id'])\
            .where('(start_timestamp_int < start_time_int and end_timestamp_int > start_time_int) or (start_timestamp_int >= start_time_int and start_timestamp_int < end_time_int)')\
            .withColumn('valid_duration', F.expr('if(start_timestamp_int < start_time_int, '
                                                 'if(end_timestamp_int < end_time_int, end_timestamp_int - start_time_int, end_time_int - start_time_int), '
                                                 'if(end_timestamp_int < end_time_int, end_timestamp_int - start_timestamp_int, end_time_int - start_timestamp_int))'))\
            .withColumn('valid_duration', F.expr('cast(valid_duration as bigint)'))\
            .groupBy('date', 'content_id')\
            .agg(F.sum('valid_duration').alias('total_duration'),
                 F.countDistinct("dw_p_id").alias('total_pid_reach'),
                 F.countDistinct("dw_d_id").alias('total_did_reach'))\
            .withColumn('total_inventory', F.expr(f'cast((total_duration / 10 * {rate}) as bigint)')) \
            .withColumn('total_pid_reach', F.expr(f'cast((total_pid_reach * {rate}) as bigint)'))\
            .withColumn('total_did_reach', F.expr(f'cast((total_did_reach * {rate}) as bigint)'))\
            .cache()
        save_data_frame(total_inventory_df, pipeline_base_path + f"/label/inventory/tournament={tournament}/contentid={content_id}")
        # total_inventory_df.where('total_inventory < 0').show()
        total_inventory_df.orderBy('content_id').show()


def save_base_dataset():
    tournament_dic = {
        'sri_lanka_tour_of_india2023': {
            'tournament_name': 'Sri Lanka Tour of India 2023',
            'tournament_type': 'Tour',
            'match_type': '',
            'venue': 'India',
            'gender_type': 'men',
            'vod_type': 'svod'
        },
        'new_zealand_tour_of_india2023': {
            'tournament_name': 'New Zealand Tour of India 2023',
            'tournament_type': 'Tour',
            'match_type': '',
            'venue': 'India',
            'gender_type': 'men',
            'vod_type': 'svod'
        },
        'wc2022': {
            'tournament_name': 'World Cup 2022',
            'tournament_type': 'International',
            'match_type': 'T20',
            'venue': 'Australia',
            'gender_type': 'men',
            'vod_type': 'svod'
        },
        'ac2022': {
            'tournament_name': 'Asia Cup 2022',
            'tournament_type': 'International',
            'match_type': 'T20',
            'venue': 'United Arab Emirates',
            'gender_type': 'men',
            'vod_type': 'svod'
        },
        'south_africa_tour_of_india2022': {
            'tournament_name': 'South Africa Tour of India 2022',
            'tournament_type': 'Tour',
            'match_type': '',
            'venue': 'India',
            'gender_type': 'men',
            'vod_type': 'svod'
        },
        'west_indies_tour_of_india2022': {
            'tournament_name': 'West Indies Tour of India 2022',
            'tournament_type': 'Tour',
            'match_type': '',
            'venue': 'India',
            'gender_type': 'men',
            'vod_type': 'svod'
        },
        'ipl2022': {
            'tournament_name': 'IPL 2022',
            'tournament_type': 'National',
            'match_type': 'T20',
            'venue': 'India',
            'gender_type': 'men',
            'vod_type': 'svod'
        },
        'england_tour_of_india2021': {
            'tournament_name': 'England Tour of India 2021',
            'tournament_type': 'Tour',
            'match_type': '',
            'venue': 'India',
            'gender_type': 'men',
            'vod_type': 'svod'
        },
        'wc2021': {
            'tournament_name': 'World Cup 2021',
            'tournament_type': 'International',
            'match_type': 'T20',
            'venue': 'United Arab Emirates',
            'gender_type': 'men',
            'vod_type': 'svod'
        },
        'ipl2021': {
            'tournament_name': 'IPL 2021',
            'tournament_type': 'National',
            'match_type': 'T20',
            'venue': 'India, United Arab Emirates',
            'gender_type': 'men',
            'vod_type': 'svod'
        },
        'australia_tour_of_india2020': {
            'tournament_name': 'Australia Tour of India 2020',
            'tournament_type': 'Tour',
            'match_type': 'ODI',
            'venue': 'India',
            'gender_type': 'men',
            'vod_type': 'avod'
        },
        'india_tour_of_new_zealand2020': {
            'tournament_name': 'India Tour of New Zealand 2020',
            'tournament_type': 'Tour',
            'match_type': '',
            'venue': 'New Zealand',
            'gender_type': 'men',
            'vod_type': 'avod'
        },
        'ipl2020': {
            'tournament_name': 'IPL 2020',
            'tournament_type': 'National',
            'match_type': 'T20',
            'venue': 'India',
            'gender_type': 'men',
            'vod_type': 'svod'
        },
        'west_indies_tour_of_india2019': {
            'tournament_name': 'West Indies Tour India 2019',
            'tournament_type': 'Tour',
            'match_type': '',
            'venue': 'India',
            'gender_type': 'men',
            'vod_type': 'avod'},
        'wc2019': {
            'tournament_name': 'World Cup 2019',
            'tournament_type': 'International',
            'match_type': 'ODI',
            'venue': 'England',
            'gender_type': 'men',
            'vod_type': 'avod'
        },
        'ipl2019': {
            'tournament_name': 'IPL 2019',
            'tournament_type': 'National',
            'match_type': 'T20',
            'venue': 'India',
            'gender_type': 'men',
            'vod_type': 'avod'
        }
    }
    tournament_list = [tournament for tournament in tournament_dic]
    tournament_list.remove('ipl2019')
    for tournament in tournament_list:
        print(tournament)
        data_source = "watched_video_sampled"
        if not check_s3_path_exist(live_ads_inventory_forecasting_root_path + f"/final_test_dataset/{data_source}_of_{tournament}"):
            data_source = "watched_video"
        df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/final_test_dataset/{data_source}_of_{tournament}")
        contents = df.select('content_id').distinct().collect()
        print(contents)
        for content in contents:
            # print(content)
            content_id = content[0]
            save_data_frame(df.where(f'content_id="{content_id}"'), pipeline_base_path + f"/label/inventory/tournament={tournament}/contentid={content_id}")


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
pipeline_base_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/pipeline"
playout_log_path_suffix = "/playout_log"

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


# save_base_dataset()
# main(spark, date, content_id, tournament_name)
