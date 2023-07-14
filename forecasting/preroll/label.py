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


def save_inventory_data(tournament):
    match_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"match_data/{tournament}").cache()
    valid_dates = match_df.select('date').orderBy('date').distinct().collect()
    # print(valid_dates)
    # print(len(valid_dates))
    complete_valid_dates = [date[0] for date in valid_dates]
    for date in valid_dates:
        next_date = get_date_list(date[0], 2)[-1]
        if next_date not in complete_valid_dates:
            complete_valid_dates.append(next_date)
    date_list = complete_valid_dates.copy()
    complete_valid_dates = []
    for date in date_list:
        if date >= "2020-05-15":
            complete_valid_dates.append(date)
    if complete_valid_dates:
        label_df = reduce(lambda x, y: x.union(y), [load_data_frame(spark, f"{aggr_shifu_inventory_path}/cd={date}")
                          .withColumn('user_account_type', F.upper(F.col('user_account_type')))
                          .withColumn('ad_placement', F.upper(F.col('ad_placement')))
                          .where('ad_placement = "PREROLL"')
                          .withColumn('sub_tag', F.locate('SUBSCRIBED_FREE', F.col('user_account_type')))
                          .withColumn('sub_tag', F.expr('if(sub_tag>0 or user_account_type="", 0, 1)'))
                          .groupBy('content_id', 'sub_tag')
                          .agg(F.sum('inventory').alias('inventory')) for date in complete_valid_dates])\
            .join(match_df, 'content_id')\
            .where(f'{content_filter1}')\
            .where(f'{content_filter2}')
        save_data_frame(label_df, f"{preroll_live_ads_inventory_forecasting_root_path}/inventory_data/aggr_inventory/tournament={tournament}")


def save_detailed_inventory_data(tournament):
    match_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"match_data/{tournament}").cache()
    valid_dates = match_df.select('date').orderBy('date').distinct().collect()
    # print(valid_dates)
    # print(len(valid_dates))
    complete_valid_dates = [date[0] for date in valid_dates]
    for date in valid_dates:
        next_date = get_date_list(date[0], 2)[-1]
        if next_date not in complete_valid_dates:
            complete_valid_dates.append(next_date)
    date_list = complete_valid_dates.copy()
    complete_valid_dates = []
    for date in date_list:
        if date >= "2020-05-15":
            complete_valid_dates.append(date)
    if complete_valid_dates:
        label_df = reduce(lambda x, y: x.union(y), [load_data_frame(spark, f"{aggr_shifu_inventory_path}/cd={date}")
                          .withColumn('user_account_type', F.upper(F.col('user_account_type')))
                          .withColumn('ad_placement', F.upper(F.col('ad_placement')))
                          .where('ad_placement = "PREROLL"')
                          .withColumn('sub_tag', F.locate('SUBSCRIBED_FREE', F.col('user_account_type')))
                          .withColumn('sub_tag', F.expr('if(sub_tag>0 or user_account_type="", 0, 1)'))
                          .select('content_id', 'content_title', 'content_type', 'content_language', 'break_no', 'sub_tag', 'user_account_type', 'inventory')
                                                    for date in complete_valid_dates])\
            .join(match_df, 'content_id')\
            .where(f'{content_filter1}')\
            .where(f'{content_filter2}')
        save_data_frame(label_df, f"{preroll_live_ads_inventory_forecasting_root_path}/inventory_data/aggr_inventory_detailed/tournament={tournament}")


live_ads_inventory_forecasting_root_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting"
preroll_live_ads_inventory_forecasting_root_path = f"{live_ads_inventory_forecasting_root_path}/preroll"
aggr_shifu_inventory_path = "s3://hotstar-ads-targeting-us-east-1-prod/trackers/shifu_reporting/aggregates/hourly/ad_inventory"
watch_video_sampled_path = "s3://hotstar-ads-ml-us-east-1-prod/data_exploration/data/data_backup/watched_video/"
live_ads_inventory_forecasting_complete_feature_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/complete_features"


tournament_list = ['australia_tour_of_india_2023', 'sri_lanka_tour_of_india2023', 'new_zealand_tour_of_india2023', 'wc2022', 'ac2022',
                   'south_africa_tour_of_india2022', 'west_indies_tour_of_india2022', 'ipl2022',
                   'england_tour_of_india2021', 'wc2021', 'ipl2021', 'australia_tour_of_india2020',
                   'india_tour_of_new_zealand2020', 'ipl2020', 'west_indies_tour_of_india2019', 'wc2019']
content_filter1 = 'content_id not in ("1440000689", "1440000694", "1440000696", "1440000982", "1540019005", "1540019014", "1540019017","1540016333")'
content_filter2 = 'date != "2022-08-24"'
# for tournament in tournament_list[:1]:
#     print(tournament)
#     save_inventory_data(tournament)

# for tournament in ['wc2021', 'wc2022']:
#     print(tournament)
#     save_detailed_inventory_data(tournament)


df = load_data_frame(spark, f"{preroll_live_ads_inventory_forecasting_root_path}/inventory_data/aggr_inventory_detailed/tournament=wc2021").cache()
df2 = load_data_frame(spark, f"{preroll_live_ads_inventory_forecasting_root_path}/inventory_data/aggr_inventory_detailed/tournament=wc2022").cache()
for col in ['content_language', 'break_no', 'sub_tag', 'user_account_type']:
    df.groupBy('date', 'content_id', col).agg(F.sum('inventory').alias('inventory')).where('date<="2021-10-22"').orderBy('date', 'content_id', col).show(2000, False)
    df2.groupBy('date', 'content_id', col).agg(F.sum('inventory').alias('inventory')).where('date<="2022-10-21"').orderBy('date', 'content_id', col).show(2000, False)


# df = load_data_frame(spark, f"{preroll_live_ads_inventory_forecasting_root_path}/inventory_data/aggr_inventory") \
#     .groupBy('date', 'content_id', 'sub_tag', 'title', 'tournament')\
#     .agg(F.sum('inventory').alias('inventory'))\
#     .cache()
# save_data_frame(df, f"{preroll_live_ads_inventory_forecasting_root_path}/inventory_data/gt_inventory_separate")
# save_data_frame(df.groupBy('date', 'content_id', 'title', 'tournament').agg(F.sum('inventory').alias('inventory')), f"{preroll_live_ads_inventory_forecasting_root_path}/inventory_data/gt_inventory")


inventory_df = load_data_frame(spark, f"{preroll_live_ads_inventory_forecasting_root_path}/inventory_data/gt_inventory")\
    .join(load_data_frame(spark, f"{preroll_live_ads_inventory_forecasting_root_path}/inventory_data/gt_inventory_separate")
          .where('sub_tag=0')
          .selectExpr('content_id', 'inventory as free_inventory'), 'content_id')\
    .join(load_data_frame(spark, f"{preroll_live_ads_inventory_forecasting_root_path}/inventory_data/gt_inventory_separate")
          .where('sub_tag=1')
          .selectExpr('content_id', 'inventory as sub_inventory'), 'content_id')\
    .orderBy('date', 'content_id')\
    .cache()
print(inventory_df.count())

path_suffix = "/all_features_hots_format_with_avg_au_sub_free_num"
wv_df = load_data_frame(spark, live_ads_inventory_forecasting_complete_feature_path + path_suffix)\
    .select('content_id', 'match_active_free_num', 'match_active_sub_num')\
    .cache()
res_df = inventory_df\
    .join(wv_df, 'content_id')\
    .withColumn('free_avg_session_num', F.expr('free_inventory/match_active_free_num'))\
    .withColumn('sub_avg_session_num', F.expr('sub_inventory/match_active_sub_num'))\
    .orderBy('date', 'content_id')\
    .cache()

print(res_df.count())
save_data_frame(res_df, f"{preroll_live_ads_inventory_forecasting_root_path}/inventory_data/gt_inventory_with_avg_session_num")


res_df.show(2000, False)



PLAY_OUT_LOG_INPUT_PATH = "s3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test"
WATCH_AGGREGATED_INPUT_PATH = "s3://hotstar-dp-datalake-processed-us-east-1-prod/aggregates/watched_video_daily_aggregates_ist_v4"
ACTIVE_USER_NUM_PATH = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_v3/truth/"
LIVE_ADS_INVENTORY_FORECASTING_ROOT_PATH = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting"
LIVE_ADS_INVENTORY_FORECASTING_COMPLETE_FEATURE_PATH = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/complete_features"
PIPELINE_BASE_PATH = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/pipeline"
TRAINING_DATA_PATH = f"{PIPELINE_BASE_PATH}/all_features_hots_format_full_avod_and_simple_one_hot_overall"
DVV_PREDICTION_PATH = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_v3/forecast/"
DVV_TRUTH_PATH = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_v3/truth/"
DVV_COMBINE_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_v3/combine/'
AVG_DVV_PATH = f"{PIPELINE_BASE_PATH}/avg_dau"
PIPELINE_DATA_TMP_PATH = f"{PIPELINE_BASE_PATH}/dataset/tmp"
TRAIN_MATCH_TABLE_PATH = f"{PIPELINE_BASE_PATH}/match_table/train"
PREDICTION_MATCH_TABLE_PATH = f"{PIPELINE_BASE_PATH}/match_table/prediction"
INVENTORY_FORECAST_REQUEST_PATH = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/inventory_forecast_input"

label_df = load_data_frame(spark, f"{preroll_live_ads_inventory_forecasting_root_path}/inventory_data/gt_inventory_with_avg_session_num")
train_df = load_data_frame(spark, TRAIN_MATCH_TABLE_PATH + f"/cd=2023-07-14")
train_df.count()
train_df.where('watch_time_per_free_per_match > 0').count()
train_df.where('frees_watching_match_rate <= 0').show(20, False)
train_df.where('frees_watching_match_rate <= 0').show(20, False)
train_df.where('reach_rate >= 0').count()
train_df.count()
label_df.where('free_avg_session_num <= 0').count()
res_df = train_df\
    .join(label_df.selectExpr('date', 'content_id', 'free_inventory as preroll_free_inventory', 'sub_inventory as preroll_sub_inventory',
                              'free_avg_session_num as preroll_free_sessions', 'sub_avg_session_num as preroll_sub_sessions'),
          ['date', 'content_id'], 'left')\
    .fillna(-1.0, ['preroll_free_inventory', 'preroll_sub_inventory', 'preroll_free_sessions', 'preroll_sub_sessions'])
print(res_df.count())
save_data_frame(res_df, TRAIN_MATCH_TABLE_PATH + f"/cd=2023-07-14")


#

# match_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"match_data/wc2021").cache()
# valid_dates = match_df.select('date').orderBy('date').distinct().collect()
# content_ids = [item[0] for item in match_df.select('content_id').distinct().collect()]
# content_ids_str = '", "'.join(content_ids)
# print(content_ids_str)
# # print(valid_dates)
# # print(len(valid_dates))
# complete_valid_dates = [date[0] for date in valid_dates]
# for date in valid_dates:
#     next_date = get_date_list(date[0], 2)[-1]
#     if next_date not in complete_valid_dates:
#         complete_valid_dates.append(next_date)
#
#
# watch_df = reduce(lambda x, y: x.union(y), [load_data_frame(spark, f"{watch_video_sampled_path}/cd={date}")
#                   .select('dw_p_id', 'dw_d_id', 'content_id', 'subscription_status', 'has_exited') for date in complete_valid_dates])\
#     .join(F.broadcast(match_df.select('content_id')), 'content_id')\
#     .withColumn('subscription_status', F.upper(F.col('subscription_status')))\
#     .withColumn('sub_tag', F.expr('if(subscription_status in ("ACTIVE", "CANCELLED", "GRACEPERIOD"), 1, 0)'))\
#     .cache()
#
# watch_df\
#     .withColumn('reload', F.expr('if(has_exited = true, 1, 0)'))\
#     .groupBy('content_id', 'sub_tag')\
#     .agg(F.countDistinct('dw_p_id').alias('reach'),
#          F.sum('reload').alias('reload_num'))\
#     .withColumn('inventory', F.expr('reach+reload_num'))\
#     .withColumn('sessions', F.expr('inventory/reach'))\
#     .orderBy('sub_tag', 'content_id')\
#     .show(200, False)
