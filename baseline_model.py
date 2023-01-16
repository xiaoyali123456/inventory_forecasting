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


def simple_title(title):
    if title.find(": ") > -1:
        title = title.split(": ")[-1]
    teams = sorted(title.split(" vs "))
    return teams[0] + " vs " + teams[1]


def estimate_inventory(rank, title, total_free_num, total_sub_num,
                       active_frees_rate, frees_watching_match_rate, watch_time_per_free_per_match,
                       active_subscribers_rate, subscribers_watching_match_rate, watch_time_per_subscriber_per_match,
                       avg_india_active_frees_rate, avg_india_frees_watching_match_rate, avg_india_watch_time_per_free_per_match,
                       avg_india_active_subscribers_rate, avg_india_subscribers_watching_match_rate, avg_india_watch_time_per_subscriber_per_match,
                       avg_stage_1_active_frees_rate, avg_stage_1_frees_watching_match_rate, avg_stage_1_watch_time_per_free_per_match,
                       avg_stage_1_active_subscribers_rate, avg_stage_1_subscribers_watching_match_rate, avg_stage_1_watch_time_per_subscriber_per_match,
                       avg_stage_2_active_frees_rate, avg_stage_2_frees_watching_match_rate, avg_stage_2_watch_time_per_free_per_match,
                       avg_stage_2_active_subscribers_rate, avg_stage_2_subscribers_watching_match_rate, avg_stage_2_watch_time_per_subscriber_per_match,
                       avg_sf_active_frees_rate, avg_sf_frees_watching_match_rate, avg_sf_watch_time_per_free_per_match,
                       avg_sf_active_subscribers_rate, avg_sf_subscribers_watching_match_rate, avg_sf_watch_time_per_subscriber_per_match,
                       avg_final_active_frees_rate, avg_final_frees_watching_match_rate, avg_final_watch_time_per_free_per_match,
                       avg_final_active_subscribers_rate, avg_final_subscribers_watching_match_rate, avg_final_watch_time_per_subscriber_per_match,
                       total_match_duration_in_minutes, number_of_ad_breaks, average_length_of_a_break_in_seconds):
    if rank == 43 or rank == 44:
        free_rate = avg_sf_active_frees_rate
        free_watch_rate = avg_sf_frees_watching_match_rate
        free_watch_time = avg_sf_watch_time_per_free_per_match
        sub_rate = avg_sf_active_subscribers_rate
        sub_watch_rate = avg_sf_subscribers_watching_match_rate
        sub_watch_time = avg_sf_watch_time_per_subscriber_per_match
        match_type = 1
    elif rank == 45:
        free_rate = avg_final_active_frees_rate
        free_watch_rate = avg_final_frees_watching_match_rate
        free_watch_time = avg_final_watch_time_per_free_per_match
        sub_rate = avg_final_active_subscribers_rate
        sub_watch_rate = avg_final_subscribers_watching_match_rate
        sub_watch_time = avg_final_watch_time_per_subscriber_per_match
        match_type = 2
    elif active_frees_rate > 0:
        free_rate = active_frees_rate
        free_watch_rate = frees_watching_match_rate
        free_watch_time = watch_time_per_free_per_match
        sub_rate = active_subscribers_rate
        sub_watch_rate = subscribers_watching_match_rate
        sub_watch_time = watch_time_per_subscriber_per_match
        match_type = 3
    elif title.find("india") > -1:
        free_rate = avg_india_active_frees_rate
        free_watch_rate = avg_india_frees_watching_match_rate
        free_watch_time = avg_india_watch_time_per_free_per_match
        sub_rate = avg_india_active_subscribers_rate
        sub_watch_rate = avg_india_subscribers_watching_match_rate
        sub_watch_time = avg_india_watch_time_per_subscriber_per_match
        match_type = 4
    elif rank <= 12:
        free_rate = avg_stage_1_active_frees_rate
        free_watch_rate = avg_stage_1_frees_watching_match_rate
        free_watch_time = avg_stage_1_watch_time_per_free_per_match
        sub_rate = avg_stage_1_active_subscribers_rate
        sub_watch_rate = avg_stage_1_subscribers_watching_match_rate
        sub_watch_time = avg_stage_1_watch_time_per_subscriber_per_match
        match_type = 5
    else:
        free_rate = avg_stage_2_active_frees_rate
        free_watch_rate = avg_stage_2_frees_watching_match_rate
        free_watch_time = avg_stage_2_watch_time_per_free_per_match
        sub_rate = avg_stage_2_active_subscribers_rate
        sub_watch_rate = avg_stage_2_subscribers_watching_match_rate
        sub_watch_time = avg_stage_2_watch_time_per_subscriber_per_match
        match_type = 6
    if rank >= 43 and title.find("india") > -1:
        free_rate = min(1.0, free_rate + 0.1)
        free_watch_rate = min(1.0, free_watch_rate + 0.1)
        sub_rate = min(1.0, sub_rate + 0.1)
        sub_watch_rate = min(1.0, sub_watch_rate + 0.1)
        free_watch_time = free_watch_time * 1.5
        sub_watch_time = sub_watch_time * 1.5
    # free_watch_time = min(free_watch_time, 5.0)
    avg_concurrency = ((float(total_free_num) * free_rate * free_watch_rate * free_watch_time)
                       + (float(total_sub_num) * sub_rate * sub_watch_rate * sub_watch_time))/total_match_duration_in_minutes
    return float(avg_concurrency * (number_of_ad_breaks * average_length_of_a_break_in_seconds / 10.0))


def estimate_reach(rank, title, total_free_num, total_sub_num,
                   active_frees_rate, frees_watching_match_rate, watch_time_per_free_per_match,
                   active_subscribers_rate, subscribers_watching_match_rate, watch_time_per_subscriber_per_match,
                   avg_india_active_frees_rate, avg_india_frees_watching_match_rate, avg_india_watch_time_per_free_per_match,
                   avg_india_active_subscribers_rate, avg_india_subscribers_watching_match_rate, avg_india_watch_time_per_subscriber_per_match,
                   avg_stage_1_active_frees_rate, avg_stage_1_frees_watching_match_rate, avg_stage_1_watch_time_per_free_per_match,
                   avg_stage_1_active_subscribers_rate, avg_stage_1_subscribers_watching_match_rate, avg_stage_1_watch_time_per_subscriber_per_match,
                   avg_stage_2_active_frees_rate, avg_stage_2_frees_watching_match_rate, avg_stage_2_watch_time_per_free_per_match,
                   avg_stage_2_active_subscribers_rate, avg_stage_2_subscribers_watching_match_rate, avg_stage_2_watch_time_per_subscriber_per_match,
                   avg_sf_active_frees_rate, avg_sf_frees_watching_match_rate, avg_sf_watch_time_per_free_per_match,
                   avg_sf_active_subscribers_rate, avg_sf_subscribers_watching_match_rate, avg_sf_watch_time_per_subscriber_per_match,
                   avg_final_active_frees_rate, avg_final_frees_watching_match_rate, avg_final_watch_time_per_free_per_match,
                   avg_final_active_subscribers_rate, avg_final_subscribers_watching_match_rate, avg_final_watch_time_per_subscriber_per_match,
                   sub_pid_did_rate, free_pid_did_rate):
    if rank == 43 or rank == 44:
        free_rate = avg_sf_active_frees_rate
        free_watch_rate = avg_sf_frees_watching_match_rate
        free_watch_time = avg_sf_watch_time_per_free_per_match
        sub_rate = avg_sf_active_subscribers_rate
        sub_watch_rate = avg_sf_subscribers_watching_match_rate
        sub_watch_time = avg_sf_watch_time_per_subscriber_per_match
    elif rank == 45:
        free_rate = avg_final_active_frees_rate
        free_watch_rate = avg_final_frees_watching_match_rate
        free_watch_time = avg_final_watch_time_per_free_per_match
        sub_rate = avg_final_active_subscribers_rate
        sub_watch_rate = avg_final_subscribers_watching_match_rate
        sub_watch_time = avg_final_watch_time_per_subscriber_per_match
    elif active_frees_rate > 0:
        free_rate = active_frees_rate
        free_watch_rate = frees_watching_match_rate
        free_watch_time = watch_time_per_free_per_match
        sub_rate = active_subscribers_rate
        sub_watch_rate = subscribers_watching_match_rate
        sub_watch_time = watch_time_per_subscriber_per_match
    elif title.find("india") > -1:
        free_rate = avg_india_active_frees_rate
        free_watch_rate = avg_india_frees_watching_match_rate
        free_watch_time = avg_india_watch_time_per_free_per_match
        sub_rate = avg_india_active_subscribers_rate
        sub_watch_rate = avg_india_subscribers_watching_match_rate
        sub_watch_time = avg_india_watch_time_per_subscriber_per_match
    elif rank <= 12:
        free_rate = avg_stage_1_active_frees_rate
        free_watch_rate = avg_stage_1_frees_watching_match_rate
        free_watch_time = avg_stage_1_watch_time_per_free_per_match
        sub_rate = avg_stage_1_active_subscribers_rate
        sub_watch_rate = avg_stage_1_subscribers_watching_match_rate
        sub_watch_time = avg_stage_1_watch_time_per_subscriber_per_match
    else:
        free_rate = avg_stage_2_active_frees_rate
        free_watch_rate = avg_stage_2_frees_watching_match_rate
        free_watch_time = avg_stage_2_watch_time_per_free_per_match
        sub_rate = avg_stage_2_active_subscribers_rate
        sub_watch_rate = avg_stage_2_subscribers_watching_match_rate
        sub_watch_time = avg_stage_2_watch_time_per_subscriber_per_match
    # if rank >= 43 and title.find("india") > -1:
    #     free_rate = min(1.0, free_rate + 0.1)
    #     free_watch_rate = min(1.0, free_watch_rate + 0.1)
    #     sub_rate = min(1.0, sub_rate + 0.1)
    #     sub_watch_rate = min(1.0, sub_watch_rate + 0.1)
    #     free_watch_time = free_watch_time * 1.5
    #     sub_watch_time = sub_watch_time * 1.5
    # free_watch_time = min(free_watch_time, 5.0)
    res = (float(total_free_num) * free_rate * free_watch_rate / sub_pid_did_rate) + (float(total_sub_num) * sub_rate * sub_watch_rate / free_pid_did_rate)
    return float(res)


def estimate_inventory_test(rank, title, total_free_num, total_sub_num,
                       active_frees_rate, frees_watching_match_rate, watch_time_per_free_per_match,
                       active_subscribers_rate, subscribers_watching_match_rate, watch_time_per_subscriber_per_match,
                       avg_india_active_frees_rate, avg_india_frees_watching_match_rate, avg_india_watch_time_per_free_per_match,
                       avg_india_active_subscribers_rate, avg_india_subscribers_watching_match_rate, avg_india_watch_time_per_subscriber_per_match,
                       avg_stage_1_active_frees_rate, avg_stage_1_frees_watching_match_rate, avg_stage_1_watch_time_per_free_per_match,
                       avg_stage_1_active_subscribers_rate, avg_stage_1_subscribers_watching_match_rate, avg_stage_1_watch_time_per_subscriber_per_match,
                       avg_stage_2_active_frees_rate, avg_stage_2_frees_watching_match_rate, avg_stage_2_watch_time_per_free_per_match,
                       avg_stage_2_active_subscribers_rate, avg_stage_2_subscribers_watching_match_rate, avg_stage_2_watch_time_per_subscriber_per_match,
                       avg_sf_active_frees_rate, avg_sf_frees_watching_match_rate, avg_sf_watch_time_per_free_per_match,
                       avg_sf_active_subscribers_rate, avg_sf_subscribers_watching_match_rate, avg_sf_watch_time_per_subscriber_per_match,
                       avg_final_active_frees_rate, avg_final_frees_watching_match_rate, avg_final_watch_time_per_free_per_match,
                       avg_final_active_subscribers_rate, avg_final_subscribers_watching_match_rate, avg_final_watch_time_per_subscriber_per_match,
                       total_match_duration_in_minutes, number_of_ad_breaks, average_length_of_a_break_in_seconds):
    if rank == 43 or rank == 44:
        free_rate = avg_sf_active_frees_rate
        free_watch_rate = avg_sf_frees_watching_match_rate
        free_watch_time = avg_sf_watch_time_per_free_per_match
        sub_rate = avg_sf_active_subscribers_rate
        sub_watch_rate = avg_sf_subscribers_watching_match_rate
        sub_watch_time = avg_sf_watch_time_per_subscriber_per_match
        match_type = 1
    elif rank == 45:
        free_rate = avg_final_active_frees_rate
        free_watch_rate = avg_final_frees_watching_match_rate
        free_watch_time = avg_final_watch_time_per_free_per_match
        sub_rate = avg_final_active_subscribers_rate
        sub_watch_rate = avg_final_subscribers_watching_match_rate
        sub_watch_time = avg_final_watch_time_per_subscriber_per_match
        match_type = 2
    elif active_frees_rate > 0:
        free_rate = active_frees_rate
        free_watch_rate = frees_watching_match_rate
        free_watch_time = watch_time_per_free_per_match
        sub_rate = active_subscribers_rate
        sub_watch_rate = subscribers_watching_match_rate
        sub_watch_time = watch_time_per_subscriber_per_match
        match_type = 3
    elif title.find("india") > -1:
        free_rate = avg_india_active_frees_rate
        free_watch_rate = avg_india_frees_watching_match_rate
        free_watch_time = avg_india_watch_time_per_free_per_match
        sub_rate = avg_india_active_subscribers_rate
        sub_watch_rate = avg_india_subscribers_watching_match_rate
        sub_watch_time = avg_india_watch_time_per_subscriber_per_match
        match_type = 4
    elif rank <= 12:
        free_rate = avg_stage_1_active_frees_rate
        free_watch_rate = avg_stage_1_frees_watching_match_rate
        free_watch_time = avg_stage_1_watch_time_per_free_per_match
        sub_rate = avg_stage_1_active_subscribers_rate
        sub_watch_rate = avg_stage_1_subscribers_watching_match_rate
        sub_watch_time = avg_stage_1_watch_time_per_subscriber_per_match
        match_type = 5
    else:
        free_rate = avg_stage_2_active_frees_rate
        free_watch_rate = avg_stage_2_frees_watching_match_rate
        free_watch_time = avg_stage_2_watch_time_per_free_per_match
        sub_rate = avg_stage_2_active_subscribers_rate
        sub_watch_rate = avg_stage_2_subscribers_watching_match_rate
        sub_watch_time = avg_stage_2_watch_time_per_subscriber_per_match
        match_type = 6
    # if rank >= 43 and title.find("india") > -1:
    #     free_rate = min(1.0, free_rate + 0.1)
    #     free_watch_rate = min(1.0, free_watch_rate + 0.1)
    #     sub_rate = min(1.0, sub_rate + 0.1)
    #     sub_watch_rate = min(1.0, sub_watch_rate + 0.1)
    #     free_watch_time = free_watch_time * 1.5
    #     sub_watch_time = sub_watch_time * 1.5
    # # free_watch_time = min(free_watch_time, 5.0)
    # avg_concurrency = ((float(total_free_num) * free_rate * free_watch_rate * free_watch_time)
    #                    + (float(
    #             total_sub_num) * sub_rate * sub_watch_rate * sub_watch_time)) / total_match_duration_in_minutes
    return float(match_type)


def load_wt_features(tournament):
    df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/free_checking_result_of_{tournament}")\
            .join(load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/sub_checking_result_of_{tournament}")
                  .withColumnRenamed('avg_watch_time', 'avg_sub_watch_time').withColumnRenamed('total_watch_time', 'total_sub_watch_time'),
                  ['date', 'content_id', 'title', 'shortsummary'])\
            .withColumn("if_contain_india_team", F.locate('india', F.col('title')))\
            .withColumn('if_contain_india_team', F.expr('if(if_contain_india_team > 0, 1, 0)'))\
            .withColumn('if_weekend', F.dayofweek(F.col('date')))\
            .withColumn('if_weekend', F.expr('if(if_weekend=1 or if_weekend = 7, 1, 0)'))\
            .withColumnRenamed('sub_num', 'total_subscribers_number')\
            .withColumnRenamed('active_sub_rate', '% active_subscribers')\
            .withColumnRenamed('match_active_sub_rate', '% subscribers_watching_match')\
            .withColumnRenamed('avg_sub_watch_time', 'watch_time_per_subscriber_per_match') \
            .withColumnRenamed('free_num', 'total_frees_number') \
            .withColumnRenamed('active_free_rate', '% active_frees') \
            .withColumnRenamed('match_active_free_rate', '% frees_watching_match') \
            .withColumnRenamed('avg_watch_time', 'watch_time_per_free_per_match')
    # date, content_id, title, shortsummary,
    # total_frees_number, active_free_num, match_active_free_num, total_free_watch_time,
    # total_subscribers_number, active_sub_num, match_active_sub_num, total_sub_watch_time,
    # % active_frees, % frees_watching_match, watch_time_per_free_per_match,
    # % active_subscribers, % subscribers_watching_match, watch_time_per_subscriber_per_match,
    # if_contain_india_team, if_weekend
    return df


def load_labels(tournament):
    data_source = "watched_video"
    df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/final_test_dataset/{data_source}_of_{tournament}")
    # date, content_id, title, shortsummary,
    # total_inventory, total_pid_reach, total_did_reach
    return df


def first_match_data(df, col):
    return df.where('rank = 1').select(col).collect()[0][0]


def last_match_data(df, col):
    return df.where(f'rank = {df.count()}').select(col).collect()[0][0]


def days_bewteen_st_and_et(st, et):
    date1 = datetime.datetime.strptime(st, "%Y-%m-%d").date()
    date2 = datetime.datetime.strptime(et, "%Y-%m-%d").date()
    return (date2 - date1).days


check_title_valid_udf = F.udf(check_title_valid, IntegerType())
simple_title_udf = F.udf(simple_title, StringType())
estimate_inventory_udf = F.udf(estimate_inventory, FloatType())
estimate_inventory_test_udf = F.udf(estimate_inventory_test, FloatType())
estimate_reach_udf = F.udf(estimate_reach, FloatType())

concurrency_root_path = "s3://hotstar-dp-datalake-processed-us-east-1-prod/hive_internal_database/concurrency.db/"
ssai_concurrency_path = f"{concurrency_root_path}/users_by_live_sports_content_by_ssai"
match_meta_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/ads_crash/match_meta"
live_ads_inventory_forecasting_root_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting"
play_out_log_input_path = "s3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test"
watchAggregatedInputPath = "s3://hotstar-dp-datalake-processed-us-east-1-prod/aggregates/watched_video_daily_aggregates_ist_v4"
viewAggregatedInputPath = "s3://hotstar-dp-datalake-processed-us-east-1-prod/aggregates/viewed_page_daily_aggregates_ist_v2"

# spark.stop()
# spark = hive_spark('statistics')
# match_df = load_hive_table(spark, "in_cms.match_update_s3")
# save_data_frame(match_df, match_meta_path)
tournament_dic = {"wc2022": "ICC Men\'s T20 World Cup 2022",
                  "ipl2022": "TATA IPL 2022",
                  "wc2021": "ICC Men\'s T20 World Cup 2021",
                  "ipl2021": "VIVO IPL 2021",
                  "wc2019": "ICC CWC 2019"}

total_match_duration_in_minutes, number_of_ad_breaks, average_length_of_a_break_in_seconds = 240.0, 50.0, 70.0

# calculation for wc 2022
# total_free_number: first-match = first match of wc 2021, rest matches are increasing by rate in match level of wc 2021
# total_sub_number: first-match = last match of ipl 2022 + normal-increasing-rate-in-date-level * days-between-ipl-2022-and-wc-2022, rest matches are increasing by rate in match level of wc 2021
# wt-related-parameters: select most similar matches of wc 2021, and use the avg value of the parameters in those matches
#     order: knockout, contains india + 10%
# total_match_duration = X min, number_of_ad_breaks and average_length_of_a_break_in_seconds used the same as wc 2021

wc2021_feature_df = load_wt_features("wc2021")\
    .withColumn('rank', F.expr('row_number() over (partition by shortsummary order by date, content_id)'))\
    .withColumn('simple_title', simple_title_udf('title'))\
    .cache()
ipl2022_feature_df = load_wt_features("ipl2022")\
    .withColumn('rank', F.expr('row_number() over (partition by shortsummary order by date, content_id)'))\
    .withColumn('simple_title', simple_title_udf('title'))\
    .cache()
wc2022_label_df = load_labels("wc2022")\
    .withColumn('rank', F.expr('row_number() over (partition by shortsummary order by date, content_id)'))\
    .withColumn('simple_title', simple_title_udf('title'))\
    .cache()

first_match_free_num = first_match_data(wc2021_feature_df, 'total_frees_number')
free_num_increasing_rate = (last_match_data(wc2021_feature_df, 'total_frees_number') - first_match_free_num) / (wc2021_feature_df.count() - 1)
first_match_sub_num = last_match_data(ipl2022_feature_df, 'total_subscribers_number') + \
                      ((first_match_data(ipl2022_feature_df, 'total_subscribers_number') - last_match_data(wc2021_feature_df, 'total_subscribers_number')) /
                       days_bewteen_st_and_et(last_match_data(wc2021_feature_df, 'date'), first_match_data(ipl2022_feature_df, 'date')))\
                      * days_bewteen_st_and_et(last_match_data(ipl2022_feature_df, 'date'), first_match_data(wc2022_label_df, 'date'))
sub_num_increasing_rate = (last_match_data(wc2021_feature_df, 'total_subscribers_number') - first_match_data(wc2021_feature_df, 'total_subscribers_number')) / (wc2021_feature_df.count() - 1)

# % active_frees, % frees_watching_match, watch_time_per_free_per_match,
# % active_subscribers, % subscribers_watching_match, watch_time_per_subscriber_per_match
india_match_df = wc2021_feature_df\
    .where('if_contain_india_team = 1 and rank < 43')\
    .groupBy('shortsummary')\
    .agg(F.avg('% active_frees').alias('avg_india_active_frees_rate'),
         F.avg('% frees_watching_match').alias('avg_india_frees_watching_match_rate'),
         F.avg('watch_time_per_free_per_match').alias('avg_india_watch_time_per_free_per_match'),
         F.avg('% active_subscribers').alias('avg_india_active_subscribers_rate'),
         F.avg('% subscribers_watching_match').alias('avg_india_subscribers_watching_match_rate'),
         F.avg('watch_time_per_subscriber_per_match').alias('avg_india_watch_time_per_subscriber_per_match'))\
    .cache()
print(india_match_df.columns)

stage_1_match_df = wc2021_feature_df\
    .where('if_contain_india_team = 0 and rank <= 12')\
    .groupBy('shortsummary')\
    .agg(F.avg('% active_frees').alias('avg_stage_1_active_frees_rate'),
         F.avg('% frees_watching_match').alias('avg_stage_1_frees_watching_match_rate'),
         F.avg('watch_time_per_free_per_match').alias('avg_stage_1_watch_time_per_free_per_match'),
         F.avg('% active_subscribers').alias('avg_stage_1_active_subscribers_rate'),
         F.avg('% subscribers_watching_match').alias('avg_stage_1_subscribers_watching_match_rate'),
         F.avg('watch_time_per_subscriber_per_match').alias('avg_stage_1_watch_time_per_subscriber_per_match'))\
    .cache()
print(stage_1_match_df.columns)

stage_2_match_df = wc2021_feature_df\
    .where('if_contain_india_team = 0 and rank >= 13 and rank <= 42')\
    .groupBy('shortsummary')\
    .agg(F.avg('% active_frees').alias('avg_stage_2_active_frees_rate'),
         F.avg('% frees_watching_match').alias('avg_stage_2_frees_watching_match_rate'),
         F.avg('watch_time_per_free_per_match').alias('avg_stage_2_watch_time_per_free_per_match'),
         F.avg('% active_subscribers').alias('avg_stage_2_active_subscribers_rate'),
         F.avg('% subscribers_watching_match').alias('avg_stage_2_subscribers_watching_match_rate'),
         F.avg('watch_time_per_subscriber_per_match').alias('avg_stage_2_watch_time_per_subscriber_per_match'))\
    .cache()
print(stage_2_match_df.columns)

sf_match_df = wc2021_feature_df\
    .where('rank in (43, 44)')\
    .groupBy('shortsummary')\
    .agg(F.avg('% active_frees').alias('avg_sf_active_frees_rate'),
         F.avg('% frees_watching_match').alias('avg_sf_frees_watching_match_rate'),
         F.avg('watch_time_per_free_per_match').alias('avg_sf_watch_time_per_free_per_match'),
         F.avg('% active_subscribers').alias('avg_sf_active_subscribers_rate'),
         F.avg('% subscribers_watching_match').alias('avg_sf_subscribers_watching_match_rate'),
         F.avg('watch_time_per_subscriber_per_match').alias('avg_sf_watch_time_per_subscriber_per_match'))\
    .cache()
print(sf_match_df.columns)

final_match_df = wc2021_feature_df\
    .where('rank in (45)')\
    .groupBy('shortsummary')\
    .agg(F.avg('% active_frees').alias('avg_final_active_frees_rate'),
         F.avg('% frees_watching_match').alias('avg_final_frees_watching_match_rate'),
         F.avg('watch_time_per_free_per_match').alias('avg_final_watch_time_per_free_per_match'),
         F.avg('% active_subscribers').alias('avg_final_active_subscribers_rate'),
         F.avg('% subscribers_watching_match').alias('avg_final_subscribers_watching_match_rate'),
         F.avg('watch_time_per_subscriber_per_match').alias('avg_final_watch_time_per_subscriber_per_match'))\
    .cache()
print(final_match_df.columns)

dynamic_parameters = ['% active_frees', '% frees_watching_match', "watch_time_per_free_per_match",
                      '% active_subscribers', '% subscribers_watching_match', "watch_time_per_subscriber_per_match"]


sub_pid_did_rate = 0.94
free_pid_did_rate = 1.02

# res_df = wc2022_label_df\
#     .withColumn('total_free_num', F.expr(f"{first_match_free_num} + (rank - 1) * {free_num_increasing_rate}"))\
#     .withColumn('total_sub_num', F.expr(f"{first_match_sub_num} + (rank - 1) * {sub_num_increasing_rate}"))\
#     .crossJoin(F.broadcast(india_match_df.drop('shortsummary')))\
#     .crossJoin(F.broadcast(stage_1_match_df.drop('shortsummary')))\
#     .crossJoin(F.broadcast(stage_2_match_df.drop('shortsummary')))\
#     .crossJoin(F.broadcast(sf_match_df.drop('shortsummary')))\
#     .crossJoin(F.broadcast(final_match_df.drop('shortsummary')))\
#     .join(wc2021_feature_df.where('rank <= 42').select('simple_title', *dynamic_parameters), 'simple_title', 'left')\
#     .fillna(-1, dynamic_parameters)\
#     .withColumn('estimated_inventory', estimate_inventory_udf('rank', 'title', 'total_free_num', 'total_sub_num',
#                                                               '% active_frees', '% frees_watching_match', "watch_time_per_free_per_match",
#                                                               '% active_subscribers', '% subscribers_watching_match', "watch_time_per_subscriber_per_match",
#                                                               'avg_india_active_frees_rate', 'avg_india_frees_watching_match_rate', 'avg_india_watch_time_per_free_per_match',
#                                                               'avg_india_active_subscribers_rate', 'avg_india_subscribers_watching_match_rate', 'avg_india_watch_time_per_subscriber_per_match',
#                                                               'avg_stage_1_active_frees_rate', 'avg_stage_1_frees_watching_match_rate', 'avg_stage_1_watch_time_per_free_per_match',
#                                                               'avg_stage_1_active_subscribers_rate', 'avg_stage_1_subscribers_watching_match_rate', 'avg_stage_1_watch_time_per_subscriber_per_match',
#                                                               'avg_stage_2_active_frees_rate', 'avg_stage_2_frees_watching_match_rate', 'avg_stage_2_watch_time_per_free_per_match',
#                                                               'avg_stage_2_active_subscribers_rate', 'avg_stage_2_subscribers_watching_match_rate', 'avg_stage_2_watch_time_per_subscriber_per_match',
#                                                               'avg_sf_active_frees_rate', 'avg_sf_frees_watching_match_rate', 'avg_sf_watch_time_per_free_per_match',
#                                                               'avg_sf_active_subscribers_rate', 'avg_sf_subscribers_watching_match_rate', 'avg_sf_watch_time_per_subscriber_per_match',
#                                                               'avg_final_active_frees_rate', 'avg_final_frees_watching_match_rate', 'avg_final_watch_time_per_free_per_match',
#                                                               'avg_final_active_subscribers_rate', 'avg_final_subscribers_watching_match_rate', 'avg_final_watch_time_per_subscriber_per_match',
#                                                               F.lit(total_match_duration_in_minutes), F.lit(number_of_ad_breaks), F.lit(average_length_of_a_break_in_seconds)))\
#     .withColumn('estimated_reach', estimate_reach_udf('rank', 'title', 'total_free_num', 'total_sub_num',
#                                                               '% active_frees', '% frees_watching_match', "watch_time_per_free_per_match",
#                                                               '% active_subscribers', '% subscribers_watching_match', "watch_time_per_subscriber_per_match",
#                                                               'avg_india_active_frees_rate', 'avg_india_frees_watching_match_rate', 'avg_india_watch_time_per_free_per_match',
#                                                               'avg_india_active_subscribers_rate', 'avg_india_subscribers_watching_match_rate', 'avg_india_watch_time_per_subscriber_per_match',
#                                                               'avg_stage_1_active_frees_rate', 'avg_stage_1_frees_watching_match_rate', 'avg_stage_1_watch_time_per_free_per_match',
#                                                               'avg_stage_1_active_subscribers_rate', 'avg_stage_1_subscribers_watching_match_rate', 'avg_stage_1_watch_time_per_subscriber_per_match',
#                                                               'avg_stage_2_active_frees_rate', 'avg_stage_2_frees_watching_match_rate', 'avg_stage_2_watch_time_per_free_per_match',
#                                                               'avg_stage_2_active_subscribers_rate', 'avg_stage_2_subscribers_watching_match_rate', 'avg_stage_2_watch_time_per_subscriber_per_match',
#                                                               'avg_sf_active_frees_rate', 'avg_sf_frees_watching_match_rate', 'avg_sf_watch_time_per_free_per_match',
#                                                               'avg_sf_active_subscribers_rate', 'avg_sf_subscribers_watching_match_rate', 'avg_sf_watch_time_per_subscriber_per_match',
#                                                               'avg_final_active_frees_rate', 'avg_final_frees_watching_match_rate', 'avg_final_watch_time_per_free_per_match',
#                                                               'avg_final_active_subscribers_rate', 'avg_final_subscribers_watching_match_rate', 'avg_final_watch_time_per_subscriber_per_match',
#                                                               F.lit(sub_pid_did_rate), F.lit(free_pid_did_rate)))\
#     .withColumn('estimated_inventory', F.expr('cast(estimated_inventory as bigint)'))\
#     .withColumn('estimated_reach', F.expr('cast(estimated_reach as bigint)'))\
#     .withColumn('inventory_bias', F.expr('(estimated_inventory - total_inventory) / total_inventory'))\
#     .withColumn('reach_bias', F.expr('(estimated_reach - total_did_reach) / total_did_reach'))\
#     .where('total_inventory > 0')\
#     .cache()
#
# wc2022_label_df\
#     .withColumn('total_free_num', F.expr(f"{first_match_free_num} + (rank - 1) * {free_num_increasing_rate}"))\
#     .withColumn('total_sub_num', F.expr(f"{first_match_sub_num} + (rank - 1) * {sub_num_increasing_rate}"))\
#     .crossJoin(F.broadcast(india_match_df.drop('shortsummary')))\
#     .crossJoin(F.broadcast(stage_1_match_df.drop('shortsummary')))\
#     .crossJoin(F.broadcast(stage_2_match_df.drop('shortsummary')))\
#     .crossJoin(F.broadcast(sf_match_df.drop('shortsummary')))\
#     .crossJoin(F.broadcast(final_match_df.drop('shortsummary')))\
#     .join(wc2021_feature_df.select('simple_title', *dynamic_parameters), 'simple_title', 'left')\
#     .fillna(-1, dynamic_parameters)\
#     .withColumn('estimated_inventory', estimate_inventory_test_udf('rank', 'title', 'total_free_num', 'total_sub_num',
#                                                               '% active_frees', '% frees_watching_match', "watch_time_per_free_per_match",
#                                                               '% active_subscribers', '% subscribers_watching_match', "watch_time_per_subscriber_per_match",
#                                                               'avg_india_active_frees_rate', 'avg_india_frees_watching_match_rate', 'avg_india_watch_time_per_free_per_match',
#                                                               'avg_india_active_subscribers_rate', 'avg_india_subscribers_watching_match_rate', 'avg_india_watch_time_per_subscriber_per_match',
#                                                               'avg_stage_1_active_frees_rate', 'avg_stage_1_frees_watching_match_rate', 'avg_stage_1_watch_time_per_free_per_match',
#                                                               'avg_stage_1_active_subscribers_rate', 'avg_stage_1_subscribers_watching_match_rate', 'avg_stage_1_watch_time_per_subscriber_per_match',
#                                                               'avg_stage_2_active_frees_rate', 'avg_stage_2_frees_watching_match_rate', 'avg_stage_2_watch_time_per_free_per_match',
#                                                               'avg_stage_2_active_subscribers_rate', 'avg_stage_2_subscribers_watching_match_rate', 'avg_stage_2_watch_time_per_subscriber_per_match',
#                                                               'avg_sf_active_frees_rate', 'avg_sf_frees_watching_match_rate', 'avg_sf_watch_time_per_free_per_match',
#                                                               'avg_sf_active_subscribers_rate', 'avg_sf_subscribers_watching_match_rate', 'avg_sf_watch_time_per_subscriber_per_match',
#                                                               'avg_final_active_frees_rate', 'avg_final_frees_watching_match_rate', 'avg_final_watch_time_per_free_per_match',
#                                                               'avg_final_active_subscribers_rate', 'avg_final_subscribers_watching_match_rate', 'avg_final_watch_time_per_subscriber_per_match',
#                                                               F.lit(total_match_duration_in_minutes), F.lit(number_of_ad_breaks), F.lit(average_length_of_a_break_in_seconds)))\
#     .orderBy('date', 'content_id')\
#     .show(200, False)

# res_df.orderBy('date', 'content_id').show(200, False)
# res_df\
#     .groupBy('shortsummary')\
#     .agg(F.sum('total_inventory').alias('total_inventory'),
#          F.sum('estimated_inventory').alias('estimated_inventory'))\
#     .withColumn('bias', F.expr('(estimated_inventory - total_inventory) / total_inventory'))\
#     .show(200, False)

filter = 1

# tournament = "wc2022"
tournament = "ipl2022"
if tournament == "wc2022":
    data_source = "watched_video"
else:
    data_source = "watched_video_sampled"
    if tournament == "ipl2021" or tournament == "ipl2022":
        total_match_duration_in_minutes = 300.0

final_playout_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/test_dataset/break_start_time_data_{filter}_of_{tournament}") \
        .withColumn('start_time_int', F.expr('cast(unix_timestamp(start_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
        .withColumn('end_time_int', F.expr('cast(unix_timestamp(end_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
        .where('end_time_int > start_time_int')\
        .withColumn('break_duration', F.expr('end_time_int - start_time_int'))\
        .groupBy('content_id')\
        .agg(F.sum('break_duration').alias('total_break_duration'))\
        .join(load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/test_dataset/{data_source}_{filter}_of_{tournament}"), ['content_id'])\
        .withColumn('avg_break_concurrency', F.expr('total_inventory * 10 / total_break_duration'))\
        .cache()

res_df = load_wt_features(tournament)\
    .withColumn('rank', F.expr('row_number() over (partition by shortsummary order by date, content_id)'))\
    .withColumn('simple_title', simple_title_udf('title'))\
    .withColumnRenamed('% active_frees', 'free_rate')\
    .withColumnRenamed('% frees_watching_match', 'free_watch_rate')\
    .withColumnRenamed('% active_subscribers', 'sub_rate')\
    .withColumnRenamed('% subscribers_watching_match', 'sub_watch_rate')\
    .withColumn('avg_concurrency', F.expr(f'((cast(total_frees_number as float) * free_rate * free_watch_rate * watch_time_per_free_per_match) '
                                          f'+ (cast(total_subscribers_number as float) * sub_rate * sub_watch_rate * watch_time_per_subscriber_per_match)) / {total_match_duration_in_minutes}'))\
    .withColumn('estimated_inventory', F.expr(f'avg_concurrency * ({number_of_ad_breaks} * {average_length_of_a_break_in_seconds} / 10.0)'))\
    .select('date', 'content_id', 'avg_concurrency', 'estimated_inventory')\
    .join(final_playout_df, ['date', 'content_id'])\
    .cache()

res_df.orderBy('date', 'content_id').show(200, False)



res_df2 = load_wt_features(tournament)\
    .withColumn('rank', F.expr('row_number() over (partition by shortsummary order by date, content_id)'))\
    .withColumn('simple_title', simple_title_udf('title'))\
    .withColumnRenamed('% active_frees', 'free_rate')\
    .withColumnRenamed('% frees_watching_match', 'free_watch_rate')\
    .withColumnRenamed('% active_subscribers', 'sub_rate')\
    .withColumnRenamed('% subscribers_watching_match', 'sub_watch_rate')\
    .withColumn('avg_concurrency', F.expr(f'((cast(total_frees_number as float) * free_rate * free_watch_rate * watch_time_per_free_per_match) '
                                          f'+ (cast(total_subscribers_number as float) * sub_rate * sub_watch_rate * watch_time_per_subscriber_per_match)) / {total_match_duration_in_minutes}'))\
    .withColumn('estimated_inventory', F.expr(f'avg_concurrency * ({number_of_ad_breaks} * {average_length_of_a_break_in_seconds} / 10.0)'))\
    .select('date', 'content_id', 'estimated_inventory')\
    .join(wc2022_label_df, ['date', 'content_id'])\
    .cache()

res_df2.orderBy('date', 'content_id').show(200, False)

res_df2\
    .groupBy('shortsummary')\
    .agg(F.sum('total_inventory').alias('total_inventory'),
         F.sum('estimated_inventory').alias('estimated_inventory'))\
    .withColumn('bias', F.expr('(estimated_inventory - total_inventory) / total_inventory'))\
    .show(200, False)

