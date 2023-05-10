import datetime
import os
import sys
from functools import reduce
import pyspark.sql.functions as F
from pyspark.shell import spark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
import pandas as pd


storageLevel = StorageLevel.DISK_ONLY
spark.sparkContext.setLogLevel('WARN')


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


def save_inventory_raw_data(tournament):
    match_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"match_data/{tournament}").cache()
    # print(match_df.count())
    match_df.orderBy('date').show(200, False)
    valid_dates = match_df.select('date').orderBy('date').distinct().collect()
    # print(valid_dates)
    # print(len(valid_dates))
    complete_valid_dates = [date[0] for date in valid_dates]
    for date in valid_dates:
        next_date = get_date_list(date[0], 2)[-1]
        if next_date not in complete_valid_dates:
            complete_valid_dates.append(next_date)
    print(complete_valid_dates)
    print(" ".join(complete_valid_dates))


def save_midroll_wv(match_df, complete_valid_dates):
    print(complete_valid_dates)
    for tag in tag_dic:
        watch_df = reduce(lambda x, y: x.union(y),
                          [load_data_frame(spark, f'{watchAggregatedInputPath}/cd={date}', fmt="orc")
                          .withColumn('subscription_status', F.upper(F.col('subscription_status')))
                          .where(f'subscription_status {tag_dic[tag]} ("ACTIVE", "CANCELLED", "GRACEPERIOD")')
                          .groupBy('dw_p_id', 'content_id')
                          .agg(F.sum('watch_time').alias('watch_time'))
                          .withColumn('date', F.lit(date)) for date in complete_valid_dates]) \
            .join(match_df, ['content_id']) \
            .cache()
        match_active_sub_df = watch_df \
            .groupBy('content_id') \
            .agg(F.countDistinct('dw_p_id').alias('match_active_sub_num'),
                 F.sum('watch_time').alias('total_watch_time')) \
            .withColumn('avg_watch_time', F.expr('total_watch_time/match_active_sub_num')) \
            .cache()
        save_data_frame(match_active_sub_df, live_ads_inventory_forecasting_root_path + f"/{tag}_checking_result_of_{tournament}")
        print(f"{tournament} midroll {tag} done!")


def save_preroll_wv(match_df, complete_valid_dates):
    complete_valid_dates = filter(lambda date: date <= "2023-02-12" or date >= "2023-03-17", complete_valid_dates)
    inventory_df = reduce(lambda x, y: x.union(y),
                      [load_data_frame(spark, f'{preroll_inventory_path}/{date}')
                          .withColumn('user_account_type', F.upper(F.col('user_account_type')))
                          .withColumn('ad_placement', F.upper(F.col('ad_placement')))
                          .where('ad_placement = "PREROLL"')
                          .withColumn('sub_tag', F.locate('SUBSCRIBED_FREE', F.col('user_account_type')))
                          .withColumn('sub_tag', F.expr('if(sub_tag>0 or user_account_type="", 0, 1)'))
                          .select('content_id', 'sub_tag', 'dw_p_id', 'dw_d_id', 'request_id', 'break_id') for date in complete_valid_dates]) \
        .groupBy('content_id', 'sub_tag')\
        .agg(F.countDistinct('dw_p_id').alias('pid_reach'),
             F.countDistinct('dw_d_id').alias('did_reach'),
             F.countDistinct('request_id').alias('request_num'),
             F.countDistinct('break_id').alias('break_num'))\
        .join(match_df, ['content_id']) \
        .cache()
    save_data_frame(inventory_df, preroll_live_ads_inventory_forecasting_root_path + f"/date_investigation/reach_and_inventory/{tournament}")
    print(f"{tournament} preroll done!")


check_title_valid_udf = F.udf(check_title_valid, IntegerType())


live_ads_inventory_forecasting_root_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting"
preroll_live_ads_inventory_forecasting_root_path = f"{live_ads_inventory_forecasting_root_path}/preroll"
preroll_inventory_path = f"{preroll_live_ads_inventory_forecasting_root_path}/inventory_data/raw_data"
shifu_inventory_root_path = "s3://hotstar-ads-targeting-us-east-1-prod/trackers/shifu_ad_events/ad_inventory/"
shifu_impression_root_path = "s3://hotstar-ads-targeting-us-east-1-prod/trackers/shifu_ad_events/ad_impression/"
match_meta_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/ads_crash/match_meta"
watchAggregatedInputPath = "s3://hotstar-dp-datalake-processed-us-east-1-prod/aggregates/watched_video_daily_aggregates_ist_v4"


# spark.stop()
# spark = hive_spark('statistics')
# match_df = load_hive_table(spark, "in_cms.match_update_s3")
# save_data_frame(match_df, match_meta_path)

# tournament = "australia_tour_of_india_2023"
# load_data_frame(spark, match_meta_path)\
#     .withColumn('date', F.expr('substring(from_unixtime(startdate), 1, 10)'))\
#     .withColumn('shortsummary', F.expr('lower(shortsummary)'))\
#     .withColumn('tag', F.locate("australia tour of india 2023", F.col('shortsummary')))\
#     .withColumn('len', F.length(F.col('shortsummary')))\
#     .where('contenttype="SPORT_LIVE" and date between "2023-01-01" and "2023-04-20" and tag > 0')\
#     .select('date', 'shortsummary', 'contentid', 'contenttype', 'len', 'title')\
#     .distinct()\
#     .orderBy('date')\
#     .show(2000, False)

# match_df = load_data_frame(spark, match_meta_path) \
#     .withColumn('date', F.expr('substring(from_unixtime(startdate), 1, 10)')) \
#     .withColumn('shortsummary', F.expr('lower(shortsummary)'))\
#     .where(f'shortsummary = "australia tour of india 2023" and contenttype="SPORT_LIVE"') \
#     .withColumn('title', F.expr('lower(title)')) \
#     .withColumn('title_valid_tag', check_title_valid_udf('title', F.lit('warm-up'), F.lit('follow on'),
#                                                          F.lit(' fuls '), F.lit('live commentary'),
#                                                          F.lit('hotstar'), F.lit('|'), F.lit('dummy'))) \
#     .where('title_valid_tag = 1') \
#     .selectExpr('date', 'contentid as content_id', 'title', 'shortsummary') \
#     .distinct() \
#     .orderBy('date') \
#     .cache()
# match_df.show(200, False)
# save_data_frame(match_df, live_ads_inventory_forecasting_root_path + f"match_data/{tournament}")

# tournament_list = ["new_zealand_tour_of_india2023", "australia_tour_of_india_2023"]
# for tournament in tournament_list:
#     print(tournament)
#     save_inventory_raw_data(tournament)
tag_dic = {'sub': "in", "free": "not in"}
tournament = "australia_tour_of_india_2023"
match_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"match_data/{tournament}").cache()
valid_dates = match_df.select('date').orderBy('date').distinct().collect()
# print(valid_dates)
# print(len(valid_dates))
complete_valid_dates = [date[0] for date in valid_dates]
for date in valid_dates:
    next_date = get_date_list(date[0], 2)[-1]
    if next_date not in complete_valid_dates:
        complete_valid_dates.append(next_date)

# save_midroll_wv(match_df, complete_valid_dates)
# save_preroll_wv(match_df, complete_valid_dates)


midroll_df = reduce(lambda x, y: x.union(y),
                    [load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/{tag}_checking_result_of_{tournament}")
                    .withColumn('sub_tag', F.lit(tag)) for tag in tag_dic])\
    .withColumn('sub_tag', F.expr('if(sub_tag="free", 0, 1)'))\
    .cache()
midroll_df.orderBy('content_id').show(200, False)
preroll_df = load_data_frame(spark, preroll_live_ads_inventory_forecasting_root_path + f"/date_investigation/reach_and_inventory/{tournament}")\
    .cache()
preroll_df.orderBy('content_id').show(200, False)

preroll_df\
    .join(midroll_df, ['content_id', 'sub_tag'])\
    .withColumn('reach_bias', F.expr('pid_reach-match_active_sub_num'))\
    .withColumn('reach_bias_rate', F.expr('reach_bias/match_active_sub_num'))\
    .withColumn('avg_session', F.expr('break_num/pid_reach'))\
    .orderBy('content_id', 'sub_tag')\
    .show(200, False)


preroll_df\
    .join(midroll_df, ['content_id', 'sub_tag'])\
    .groupBy('content_id')\
    .agg(F.sum('pid_reach').alias('pid_reach'),
         F.sum('match_active_sub_num').alias('match_active_sub_num'),
         F.sum('break_num').alias('break_num'))\
    .withColumn('reach_bias', F.expr('pid_reach-match_active_sub_num'))\
    .withColumn('reach_bias_rate', F.expr('reach_bias/match_active_sub_num'))\
    .withColumn('avg_session', F.expr('break_num/pid_reach'))\
    .orderBy('content_id')\
    .show(200, False)




