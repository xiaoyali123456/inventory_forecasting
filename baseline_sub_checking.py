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


def check_s3_path_exist_simple(s3_path: str) -> bool:
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


def calculate_sub_num_on_target_date(sub_df, user_meta_df, target_date):
    sub_df = sub_df\
        .where(f'sub_start_time <= "{target_date}" and sub_end_time >= "{target_date}"')\
        .select('hid')\
        .distinct()\
        .join(user_meta_df, 'hid') # advance
    return sub_df.select('dw_p_id').distinct().count()


check_title_valid_udf = F.udf(check_title_valid, IntegerType())


concurrency_root_path = "s3://hotstar-dp-datalake-processed-us-east-1-prod/hive_internal_database/concurrency.db/"
ssai_concurrency_path = f"{concurrency_root_path}/users_by_live_sports_content_by_ssai"
match_meta_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/ads_crash/match_meta"
live_ads_inventory_forecasting_root_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting"
play_out_log_input_path = "s3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test"
watchAggregatedInputPath = "s3://hotstar-dp-datalake-processed-us-east-1-prod/aggregates/watched_video_daily_aggregates_ist_v4"

# spark.stop()
# spark = hive_spark('statistics')
# match_df = load_hive_table(spark, "in_cms.match_update_s3")
# save_data_frame(match_df, match_meta_path)

tournament_dic = {"wc2022": ['"ICC Men\'s T20 World Cup 2022"'],
                  "ac2022": ['"DP World Asia Cup 2022"'],
                  "ipl2022": ['"TATA IPL 2022"'],
                  "wc2021": ['"ICC Men\'s T20 World Cup 2021"'],
                  "ipl2021": ['"VIVO IPL 2021"'],
                  "ipl2020": ['"Dream11 IPL 2020"'],
                  "wc2019": ['"ICC CWC 2019"'],
                  "west_indies_tour_of_india2019": ['"West Indies Tour India 2019"', '"West Indies Tour of India 2019"'],
                  "australia_tour_of_india2020": ['"Australia Tour of India 2020"', '"Australia tour of India 2020"'],
                  "india_tour_of_new_zealand2020": ['"India Tour of New Zealand 2020"', '"India tour of New Zealand 2020"'],
                  "england_tour_of_india2021": ['"England Tour of India 2021"'],
                  "south_africa_tour_of_india2022": ['"South Africa Tour of India 2022"'],
                  "west_indies_tour_of_india2022": ['"West Indies Tour of India 2022"'],
                  "sri_lanka_tour_of_india2023": ['"Sri Lanka Tour of India 2023"'],
                  "new_zealand_tour_of_india2023": ['"New Zealand Tour of India 2023"'],
                  "ipl2019": ['"VIVO IPL 2019"']}

tournament_dic_2 = {}
for tournament in tournament_dic:
    if tournament == "england_tour_of_india2021":
        tournament_dic_2[tournament] = "1540005184|1540005193|1540005196|1540005199|1540005211|1540005214"
        tournament_dic_2[tournament] = ",".join("\""+contentid+"\"" for contentid in tournament_dic_2[tournament].split("|"))
        print(tournament_dic_2[tournament])
    else:
        tournament_dic_2[tournament] = ""

tournament = "wc2019"
# tournament = "ac2022"
# tournament = "ipl2022"
# tournament = "wc2021"
# tournament = "ipl2021"
# tournament = "wc2019"
# tournament = "west_indies_tour_of_india2019"
# tournament = "australia_tour_of_india2020"
# tournament = "india_tour_of_new_zealand2020"  # not solved
# tournament = "england_tour_of_india2021"
# tournament = "ipl2020"
# tournament = "south_africa_tour_of_india2022"
# tournament = "west_indies_tour_of_india2022"
# tournament = "sri_lanka_tour_of_india2023"
# tournament = "new_zealand_tour_of_india2023"
# tournament = "ipl2019"


subscription_status_col = "_col3"
dw_p_id_col = "_col0"
content_id_col = "_col2"
watch_time_col = "_col27"


# if check_s3_path_exist(live_ads_inventory_forecasting_root_path + f"match_data/{tournament}"):
#     match_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"match_data/{tournament}").cache()
# else:
#     if tournament_dic_2[tournament] == "":
#         match_df = load_data_frame(spark, match_meta_path)\
#             .withColumn('date', F.expr('substring(from_unixtime(startdate), 1, 10)'))\
#             .withColumn('contenttype', F.expr('if(contentid="1540002358", "SPORT_LIVE", contenttype)'))\
#             .where(f'shortsummary in ({",".join(tournament_dic[tournament])}) and contenttype="SPORT_LIVE"')\
#             .withColumn('date', F.expr('if(contentid="1540019056", "2022-11-06", date)'))\
#             .withColumn('title', F.expr('lower(title)'))\
#             .withColumn('title_valid_tag', check_title_valid_udf('title', F.lit('warm-up'), F.lit('follow on'),
#                                                                  F.lit(' fuls '), F.lit('live commentary'), F.lit('hotstar')))\
#             .where('title_valid_tag = 1')\
#             .selectExpr('date', 'contentid as content_id', 'title', 'shortsummary') \
#             .distinct() \
#             .orderBy('date')\
#             .cache()
#     else:
#         match_df = load_data_frame(spark, match_meta_path) \
#             .withColumn('date', F.expr('substring(from_unixtime(startdate), 1, 10)')) \
#             .where(f'(shortsummary in ({",".join(tournament_dic[tournament])}) and contenttype="SPORT_LIVE") or (contentid in ({tournament_dic_2[tournament]}))') \
#             .withColumn('date', F.expr('if(contentid="1540019056", "2022-11-06", date)')) \
#             .withColumn('title', F.expr('lower(title)')) \
#             .withColumn('title_valid_tag', check_title_valid_udf('title', F.lit('warm-up'), F.lit('follow on'),
#                                                                  F.lit(' fuls '), F.lit('live commentary'),
#                                                                  F.lit('hotstar'), F.lit('|'))) \
#             .where('title_valid_tag = 1') \
#             .selectExpr('date', 'contentid as content_id', 'title', 'shortsummary') \
#             .distinct() \
#             .orderBy('date') \
#             .cache()
#     save_data_frame(match_df, live_ads_inventory_forecasting_root_path + f"match_data/{tournament}")
#
# #
# # load_data_frame(spark, match_meta_path)\
# #     .withColumn('date', F.expr('substring(from_unixtime(startdate), 1, 10)'))\
# #     .withColumn('tag', F.locate("IPL", F.col('shortsummary')))\
# #     .withColumn('len', F.length(F.col('shortsummary')))\
# #     .where('contenttype="SPORT_LIVE" and date between "2019-01-18" and "2019-05-12" and tag > 0')\
# #     .select('date', 'shortsummary', 'contentid', 'contenttype', 'len')\
# #     .distinct()\
# #     .orderBy('date')\
# #     .show(2000, False)
#
# # load_data_frame(spark, match_meta_path)\
# #     .withColumn('date', F.expr('substring(from_unixtime(startdate), 1, 10)'))\
# #     .where('contentid="1540002358"')\
# #     .select('date', 'shortsummary', 'title', 'contenttype', 'contentid')\
# #     .orderBy('shortsummary', 'date')\
# #     .distinct()\
# #     .show(2000, False)
#
# print(match_df.count())
# match_df.orderBy('date').show(200, False)
# valid_dates = match_df.select('date').orderBy('date').distinct().collect()
# print(valid_dates)
# print(len(valid_dates))
#
# print(valid_dates)
# complete_valid_dates = [date[0] for date in valid_dates]
# for date in valid_dates:
#     next_date = get_date_list(date[0], 2)[-1]
#     if next_date not in complete_valid_dates:
#         complete_valid_dates.append(next_date)
#
# print(complete_valid_dates)
# print(len(complete_valid_dates))

# spark.stop()
# spark = hive_spark('statistics')
#
# user_meta_df = load_hive_table(spark, "in_ums.user_umfnd_s3") \
#     .select('pid', 'joinedon', 'userstatus', 'usertype', 'signupcountrycode') \
#     .withColumn('pid', F.expr('sha2(pid, 256)'))\
#     .withColumnRenamed('pid', 'dw_p_id')\
#     .distinct()\
#     .cache()
# save_data_frame(user_meta_df, live_ads_inventory_forecasting_root_path + f"/hid_pid_mapping_with_join_time_and_status")
#
# user_meta_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/hid_pid_mapping_with_join_time_and_status").cache()
# user_meta_df.groupBy('userstatus', 'dw_p_id').count().where('count > 1').orderBy('count', ascending=False).show()
# user_meta_df.groupBy('usertype').count().show()
# user_meta_df.groupBy('signupcountrycode').count().show()
# user_meta_df.where('dw_p_id="0b08e0e23e58a7c1e5cfe905641b744f43a1db1250e8cb4b62c99d801989f301"').show()
# df = user_meta_df\
#     .where('userstatus != "guest"')\
#     .withColumn('joinedon', F.from_unixtime(F.unix_timestamp(F.col('joinedon'), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))) \
#     .withColumn('joinedon', F.expr('substring(from_utc_timestamp(joinedon, "IST"), 1, 10)'))\
#     .groupBy('joinedon')\
#     .count()\
#     .withColumn('tag', F.lit(1))\
#     .cache()
# df\
#     .where(f'joinedon <= "2022-06-01"')\
#     .groupBy('tag')\
#     .agg(F.sum('count').alias('user_num'))\
#     .show()
# user_meta_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/hid_pid_mapping_with_join_time").cache()
# user_meta_df.groupBy('hid').count().where('count>1').show(20, False)
# user_meta_df\
#     .withColumn('time', F.from_unixtime(F.unix_timestamp(F.col('joinedon'), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")))\
#     .withColumn('ist_time', F.expr('substring(from_utc_timestamp(time, "IST"), 1, 10)'))\
#     .show(20, False)
# user_meta_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/hid_pid_mapping_with_join_time") \
#     .withColumn('joinedon', F.from_unixtime(F.unix_timestamp(F.col('joinedon'), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))) \
#     .withColumn('joinedon', F.expr('substring(from_utc_timestamp(joinedon, "IST"), 1, 10)'))\
#     .select('dw_p_id', 'joinedon')\
#     .distinct()\
#     .cache()
# save_data_frame(user_meta_df, live_ads_inventory_forecasting_root_path + f"/hid_pid_mapping_with_join_date")
# sub_num_df = reduce(lambda x, y: x.union(y), [load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/sub_num_table_for_{tournament}") for tournament in tournament_dic])\
#     .withColumnRenamed('date', 'cd')
# sub_num_df.write.partitionBy('cd').mode('overwrite').parquet(live_ads_inventory_forecasting_root_path + "/sub_num")

#
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
#     .withColumn('sub_end_time', F.expr('substring(from_utc_timestamp(sub_end_time, "IST"), 1, 10)'))
# save_data_frame(sub_df, live_ads_inventory_forecasting_root_path + f"/sub_table_valid")
# user_meta_df = load_hive_table(spark, "in_ums.user_umfnd_s3") \
#     .select('pid', 'hid', 'joinedon') \
#     .withColumn('pid', F.expr('sha2(pid, 256)'))\
#     .withColumnRenamed('pid', 'dw_p_id')\
#     .distinct()\
#     .cache()
# # # user_meta_df.groupBy('hid').count().where('count>1').show(20, False)
# save_data_frame(user_meta_df, live_ads_inventory_forecasting_root_path + f"/hid_pid_mapping")
user_meta_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/hid_pid_mapping") \
    .cache()
sub_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/sub_table_valid").cache()
for date in get_date_list("2019-05-08", 1370):
    if date <= "2022-01-15" or check_s3_path_exist_simple(live_ads_inventory_forecasting_root_path + f"/sub_num/cd={date}"):
        continue
    sub_num = calculate_sub_num_on_target_date(sub_df, user_meta_df, date)
    df = spark.createDataFrame([(sub_num, )], ["sub_num"])
    print(f"{date}: {sub_num}")
    save_data_frame(df, live_ads_inventory_forecasting_root_path + f"/sub_num/cd={date}")


user_meta_df = load_hive_table(spark, "in_ums.user_umfnd_s3") \
    .select('pid', 'joinedon', 'userstatus', 'usertype', 'signupcountrycode') \
    .withColumn('pid', F.expr('sha2(pid, 256)'))\
    .withColumnRenamed('pid', 'dw_p_id')\
    .distinct()\
    .cache()
save_data_frame(user_meta_df, live_ads_inventory_forecasting_root_path + f"/hid_pid_mapping_with_join_time_and_status")
user_meta_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/hid_pid_mapping_with_join_time_and_status") \
    .where('userstatus != "guest"')\
    .withColumn('joinedon', F.from_unixtime(F.unix_timestamp(F.col('joinedon'), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))) \
    .withColumn('joinedon', F.expr('substring(from_utc_timestamp(joinedon, "IST"), 1, 10)'))\
    .select('dw_p_id', 'joinedon')\
    .distinct()\
    .groupBy('joinedon')\
    .count()\
    .withColumn('tag', F.lit(1))\
    .cache()
for date in get_date_list("2019-05-08", 1370):
    print(date)
    df = user_meta_df\
        .where(f'joinedon <= "{date}"')\
        .groupBy('tag')\
        .agg(F.sum('count').alias('user_num'))\
        .select('user_num')
    if date <= "2019-05-30":
        df.show()
    save_data_frame(df, live_ads_inventory_forecasting_root_path + f"/user_num/cd={date}")


# user_meta_df = load_hive_table(spark, "in_ums.user_umfnd_s3") \
#     .select('pid', 'hid', 'joinedon') \
#     .withColumn('pid', F.expr('sha2(pid, 256)'))\
#     .withColumnRenamed('pid', 'dw_p_id')\
#     .distinct()\
#     .cache()
# # # user_meta_df.groupBy('hid').count().where('count>1').show(20, False)
# save_data_frame(user_meta_df, live_ads_inventory_forecasting_root_path + f"/hid_pid_mapping")
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
#     .withColumn('sub_end_time', F.expr('substring(from_utc_timestamp(sub_end_time, "IST"), 1, 10)'))
# save_data_frame(sub_df, live_ads_inventory_forecasting_root_path + f"/sub_table_valid")

user_meta_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/hid_pid_mapping").cache()
sub_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/sub_table_valid").cache()
sub_num_list = []
for date in valid_dates:
    print(date[0])
    sub_num_list.append((date[0], calculate_sub_num_on_target_date(sub_df, user_meta_df, date[0])))

sub_num_df = spark.createDataFrame(sub_num_list, ["date", "sub_num"]).orderBy('date')
save_data_frame(sub_num_df, live_ads_inventory_forecasting_root_path + f"/sub_num_table_for_{tournament}")


sub_num_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/sub_num_table_for_{tournament}").cache()
sub_num_df.show(20, False)

watch_df = reduce(lambda x, y: x.union(y),
    [load_data_frame(spark, f'{watchAggregatedInputPath}/cd={date}', fmt="orc")
        .withColumnRenamed(subscription_status_col, 'subscription_status')
        .withColumnRenamed(dw_p_id_col, 'dw_p_id')
        .withColumnRenamed(content_id_col, 'content_id')
        .withColumnRenamed(watch_time_col, 'watch_time')
        .withColumn('subscription_status', F.upper(F.col('subscription_status')))
        .where('subscription_status in ("ACTIVE", "CANCELLED", "GRACEPERIOD")')
        .groupBy('dw_p_id', 'content_id')
        .agg(F.sum('watch_time').alias('watch_time'))
        .withColumn('date', F.lit(date)) for date in complete_valid_dates]) \
    .join(match_df, ['date', 'content_id'], 'left')\
    .cache()

#
valid_contents = match_df.select('date', 'content_id').distinct().orderBy('date').collect()
active_sub_list = []
for item in valid_contents:
    print(item)
    num = watch_df\
        .where(f'date="{item[0]}" and (content_id = "{item[1]}" or title is null)') \
        .groupBy('date')\
        .agg(F.countDistinct('dw_p_id').alias('active_sub_num'))\
        .select('active_sub_num').collect()[0][0]
    print(item)
    active_sub_list.append((item[0], item[1], num))
    print(active_sub_list[-1])
    print(len(active_sub_list))

active_sub_df = spark.createDataFrame(active_sub_list, ['date', 'content_id', 'active_sub_num']).cache()
save_data_frame(active_sub_df, live_ads_inventory_forecasting_root_path + f"/active_sub_table_for_{tournament}")

active_sub_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/active_sub_table_for_{tournament}")
# active_sub_df = watch_df.groupBy('date').agg(F.countDistinct('dw_p_id').alias('active_sub_num')).cache()
match_active_sub_df = watch_df\
    .groupBy('content_id')\
    .agg(F.countDistinct('dw_p_id').alias('match_active_sub_num'),
         F.sum('watch_time').alias('total_watch_time'))\
    .withColumn('avg_watch_time', F.expr('total_watch_time/match_active_sub_num'))\
    .cache()

res_df = sub_num_df\
    .join(active_sub_df, 'date')\
    .join(match_active_sub_df, ['content_id'])\
    .withColumn('active_sub_rate', F.expr('active_sub_num/sub_num'))\
    .withColumn('match_active_sub_rate', F.expr('match_active_sub_num/active_sub_num'))\
    .join(match_df, ['date', 'content_id'])\
    .orderBy('content_id')\
    .cache()

save_data_frame(res_df, live_ads_inventory_forecasting_root_path + f"/sub_checking_result_of_{tournament}")
res_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/sub_checking_result_of_{tournament}")
res_df.orderBy('date').show(200, False)
print(f"{tournament} done!")


