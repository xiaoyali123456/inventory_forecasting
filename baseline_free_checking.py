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


check_title_valid_udf = F.udf(check_title_valid, IntegerType())


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

# tournament = "wc2022"
# tournament = "ipl2022"
# tournament = "wc2021"
tournament = "ipl2021"
# tournament = "wc2019"

segment = "content_language"
# segment = "none_content_language"
tournament_list = ["wc2022", "wc2021", "ipl2022", "ipl2021"]

subscription_status_col = "_col3"
dw_p_id_col = "_col0"
dw_d_id_col = "_col1"
content_id_col = "_col2"
watch_time_col = "_col27"

# df = reduce(lambda x, y: x.union(y),
#            [load_data_frame(spark,
#                             live_ads_inventory_forecasting_root_path + f"/all_viewers/cd={previous_date}").cache()
#             for previous_date in get_date_list("2019-05-01", 61)]).distinct()
# df.withColumn('size', F.length(F.col('dw_p_id'))).groupBy('size').count().orderBy('count', ascending=False).show(2000)
# print(df.count())

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
print(len(valid_dates))
print(match_df.count())
# match_df.show(200, False)
# if tournament == "ipl2021":
#     valid_dates = [date for date in valid_dates if date[0] >= "2021-09-21" and date[0] != "2021-09-26"][:-1]

print(valid_dates)
start_date, end_date = (valid_dates[0][0], valid_dates[-1][0])
complete_valid_dates = []
date = start_date
while True:
    if date <= end_date:
        if "2021-05-04" < date < "2021-09-19":
            date = get_date_list(date, 2)[-1]
            continue
        complete_valid_dates.append(date)
        date = get_date_list(date, 2)[-1]
    else:
        complete_valid_dates.append(date)
        break

print(complete_valid_dates)

# reduce(lambda x, y: x.union(y),
#     [load_data_frame(spark, f'{watchAggregatedInputPath}/cd={date[0]}', fmt="orc")
#         .withColumnRenamed(subscription_status_col, 'subscription_status')
#         .withColumnRenamed(dw_p_id_col, 'dw_p_id')
#         .withColumnRenamed(content_id_col, 'content_id')
#         .withColumn('subscription_status', F.upper(F.col('subscription_status')))
#         .where('subscription_status in ("ACTIVE", "CANCELLED", "GRACEPERIOD")')
#         .groupBy('content_id')
#         .agg(F.countDistinct("dw_p_id").alias('total_pid_reach'),
#             F.countDistinct("dw_d_id").alias('total_did_reach'))
#         .withColumn('date', F.lit(date[0])) for date in valid_dates]) \
#     .join(match_df.select('date', 'content_id'), ['date', 'content_id'])\
#     .withColumn('rate', F.expr('total_pid_reach/total_did_reach'))\
#     .orderBy('date', 'content_id')\
#     .show(200, False)


# user_meta_df = load_hive_table(spark, "in_ums.user_umfnd_s3") \
#     .select('pid', 'hid') \
#     .withColumn('pid', F.expr('sha2(pid, 256)'))\
#     .withColumnRenamed('pid', 'dw_p_id')\
#     .distinct()\
#     .cache()
# user_meta_df.groupBy('hid').count().where('count>1').show(20, False)
# save_data_frame(user_meta_df, live_ads_inventory_forecasting_root_path + f"/hid_pid_mapping")
user_meta_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/hid_pid_mapping")
# sub_df = load_hive_table(spark, "in_hspay_subscriptions.subscriptions_s3")\
#     .select('expiry_time', 'created_on', 'hid', 'is_deleted', 'status')\
#     .cache()
# save_data_frame(sub_df, live_ads_inventory_forecasting_root_path + f"/sub_table")

# sub_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/sub_table")\
#     .where('status in ("ACTIVE", "CANCELLED", "EXPIRED", "GRACE")')\
#     .withColumn('sub_start_time', F.expr("from_unixtime(created_on/1000)"))\
#     .withColumn('sub_end_time', F.expr("from_unixtime(expiry_time/1000)"))\
#     .select('sub_start_time', 'sub_end_time', 'hid')\
#     .withColumn('sub_start_time', F.expr('substring(from_utc_timestamp(sub_start_time, "IST"), 1, 10)'))\
#     .withColumn('sub_end_time', F.expr('substring(from_utc_timestamp(sub_end_time, "IST"), 1, 10)'))\
#     .cache()
# sub_df.show(20, False)

free_num_list = []
ready_date_dic = {}
# for date in valid_dates:
#     print(date[0])
#     # for previous_date in get_date_list(date[0], -90):
#     #     # if not check_s3_path_exist(live_ads_inventory_forecasting_root_path + f"/all_viewers/cd={previous_date}"):
#     #     if previous_date not in ready_date_dic:
#     #         print(f"- {previous_date}")
#     #         viewers_df = load_data_frame(spark, f'{viewAggregatedInputPath}/cd={previous_date}', fmt="orc")\
#     #             .withColumnRenamed(dw_p_id_col, 'dw_p_id')\
#     #             .select('dw_p_id')\
#     #             .distinct()
#     #         save_data_frame(viewers_df, live_ads_inventory_forecasting_root_path + f"/all_viewers/cd={previous_date}")
#     #         ready_date_dic[previous_date] = 1
#     df = reduce(lambda x, y: x.union(y),
#            [load_data_frame(spark,
#                             live_ads_inventory_forecasting_root_path + f"/all_viewers/cd={previous_date}").cache()
#             for previous_date in get_date_list(date[0], -90)]).distinct()
#     save_data_frame(df, live_ads_inventory_forecasting_root_path + f"/all_viewers_aggr_with_90_days/cd={date[0]}")

# print("all viewer data ready.")
#
# for date in valid_dates:
#     print(date[0])
#     sub_on_target_date_df = calculate_sub_num_on_target_date(sub_df, user_meta_df, date[0])
#     free_num = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/all_viewers_aggr_with_90_days/cd={date[0]}")\
#         .join(sub_on_target_date_df, 'dw_p_id', 'left_anti')\
#         .count()
#     free_num_list.append((date[0], free_num))
#
#
# free_num_df = spark.createDataFrame(free_num_list, ["date", "free_num"]).orderBy('date')
# save_data_frame(free_num_df, live_ads_inventory_forecasting_root_path + f"/free_num_table_for_{tournament}")

free_num_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/free_num_table_for_{tournament}").cache()
# free_num_df.show(50, False)

watch_df = reduce(lambda x, y: x.union(y),
    [load_data_frame(spark, f'{watchAggregatedInputPath}/cd={date}', fmt="orc")
        .withColumnRenamed(subscription_status_col, 'subscription_status')
        .withColumnRenamed(dw_p_id_col, 'dw_p_id')
        .withColumnRenamed(content_id_col, 'content_id')
        .withColumnRenamed(watch_time_col, 'watch_time')
        .withColumn('subscription_status', F.upper(F.col('subscription_status')))
        .where('subscription_status not in ("ACTIVE", "CANCELLED", "GRACEPERIOD")')
        .groupBy('dw_p_id', 'content_id')
        .agg(F.sum('watch_time').alias('watch_time'))
        .withColumn('date', F.lit(date)) for date in complete_valid_dates]) \
    .join(match_df, ['date', 'content_id'], 'left')\
    .cache()
# valid_contents = match_df.select('date', 'content_id').distinct().collect()
# active_free_list = []
# for item in valid_contents:
#     num = watch_df\
#         .where(f'date="{item[0]}" and (content_id = "{item[1]}" or title is null)') \
#         .groupBy('date')\
#         .agg(F.countDistinct('dw_p_id').alias('active_free_num'))\
#         .select('active_free_num').collect()[0][0]
#     active_free_list.append((item[0], item[1], num))
#     print(active_free_list[-1])
#     print(len(active_free_list))
#
# active_free_df = spark.createDataFrame(active_free_list, ['date', 'content_id', 'active_free_num']).cache()
# save_data_frame(active_free_df, live_ads_inventory_forecasting_root_path + f"/active_free_table_for_{tournament}")

active_free_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/active_free_table_for_{tournament}")
match_active_free_df = watch_df\
    .groupBy('content_id')\
    .agg(F.countDistinct('dw_p_id').alias('match_active_free_num'),
         F.sum('watch_time').alias('total_free_watch_time'))\
    .withColumn('avg_watch_time', F.expr('total_free_watch_time/match_active_free_num'))\
    .cache()

res_df = free_num_df\
    .join(active_free_df, 'date')\
    .join(match_active_free_df, ['content_id'])\
    .withColumn('active_free_rate', F.expr('active_free_num/free_num'))\
    .withColumn('match_active_free_rate', F.expr('match_active_free_num/active_free_num'))\
    .join(match_df, ['date', 'content_id'])\
    .orderBy('content_id')\
    .cache()

save_data_frame(res_df, live_ads_inventory_forecasting_root_path + f"/free_checking_result_of_{tournament}")
# res_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/free_checking_result_of_{tournament}")
res_df.orderBy('date', 'content_id').show(200, False)
print(f"{tournament} done!")


