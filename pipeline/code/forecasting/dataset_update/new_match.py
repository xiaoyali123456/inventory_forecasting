import pyspark.sql.functions as F
from pyspark.sql.types import *
import pandas as pd

from path import *
from util import *


# calculate free/sub reach and avg_wt
def calculate_reach_and_wt_from_wv_table(spark, date):
    match_sub_df = load_data_frame(spark, f'{WATCH_AGGREGATED_INPUT_PATH}/cd={date}', fmt="orc") \
        .where(f'upper(subscription_status) in ("ACTIVE", "CANCELLED", "GRACEPERIOD")') \
        .groupBy('dw_p_id', 'content_id') \
        .agg(F.sum('watch_time').alias('watch_time')) \
        .groupBy('content_id') \
        .agg(F.countDistinct('dw_p_id').alias('match_active_sub_num'),
             F.sum('watch_time').alias('total_watch_time')) \
        .withColumn('watch_time_per_subscriber_per_match', F.expr('total_watch_time/match_active_sub_num')) \
        .select('content_id', 'match_active_sub_num', 'watch_time_per_subscriber_per_match') \
        .cache()
    match_free_df = load_data_frame(spark, f'{WATCH_AGGREGATED_INPUT_PATH}/cd={date}', fmt="orc") \
        .where(f'upper(subscription_status) not in ("ACTIVE", "CANCELLED", "GRACEPERIOD")') \
        .groupBy('dw_p_id', 'content_id') \
        .agg(F.sum('watch_time').alias('watch_time')) \
        .groupBy('content_id') \
        .agg(F.countDistinct('dw_p_id').alias('match_active_free_num'),
             F.sum('watch_time').alias('total_free_watch_time')) \
        .withColumn('watch_time_per_free_per_match', F.expr('total_free_watch_time/match_active_free_num')) \
        .select('content_id', 'match_active_free_num', 'watch_time_per_free_per_match') \
        .cache()
    return match_sub_df, match_free_df


# get break list with break_start_time, break_end_time
def break_info_processing(playout_df, date):
    cols = ['content_id', 'break_start_time_int', 'break_end_time_int']
    playout_df = playout_df \
        .withColumn('rank', F.expr('row_number() over (partition by content_id order by break_start_time_int)')) \
        .withColumn('rank_next', F.expr('rank+1'))
    res_df = playout_df \
        .join(playout_df.selectExpr('content_id', 'rank_next as rank', 'break_end_time_int as break_end_time_int_next'),
              ['content_id', 'rank']) \
        .withColumn('bias', F.expr('break_start_time_int - break_end_time_int_next')) \
        .where('bias >= 0') \
        .orderBy('break_start_time_int')
    res_df = playout_df \
        .where('rank = 1') \
        .select(*cols) \
        .union(res_df.select(*cols))
    save_data_frame(res_df, PIPELINE_BASE_PATH + f"/label/break_info/cd={date}")
    res_df = load_data_frame(spark, PIPELINE_BASE_PATH + f"/label/break_info/cd={date}")
    return res_df


# reformat playout logs
def reformat_playout_df(playout_df):
    return playout_df\
        .withColumnRenamed(CONTENT_ID_COL2, 'content_id')\
        .withColumnRenamed(START_TIME_COL2, 'start_time')\
        .withColumnRenamed(END_TIME_COL2, 'end_time')\
        .withColumnRenamed(PLATFORM_COL2, 'platform')\
        .withColumnRenamed(TENANT_COL2, 'tenant')\
        .withColumnRenamed(CONTENT_LANGUAGE_COL2, 'content_language')\
        .withColumnRenamed(CREATIVE_ID_COL2, 'creative_id')\
        .withColumnRenamed(BREAK_ID_COL2, 'break_id')\
        .withColumnRenamed(PLAYOUT_ID_COL2, 'playout_id')\
        .withColumnRenamed(CREATIVE_PATH_COL2, 'creative_path')\
        .withColumnRenamed(CONTENT_ID_COL, 'content_id')\
        .withColumnRenamed(START_TIME_COL, 'start_time')\
        .withColumnRenamed(END_TIME_COL, 'end_time')\
        .withColumnRenamed(PLATFORM_COL, 'platform')\
        .withColumnRenamed(TENANT_COL, 'tenant')\
        .withColumnRenamed(CONTENT_LANGUAGE_COL, 'content_language')\
        .withColumnRenamed(CREATIVE_ID_COL, 'creative_id')\
        .withColumnRenamed(BREAK_ID_COL, 'break_id')\
        .withColumnRenamed(PLAYOUT_ID_COL, 'playout_id')\
        .withColumnRenamed(CREATIVE_PATH_COL, 'creative_path') \
        .withColumn('content_id', F.trim(F.col('content_id')))


@F.udf(returnType=TimestampType())
def parse_timestamp(date: str, ts: str):
    return pd.Timestamp(date + ' ' + ts)


# playout data processing
def playout_data_processing(spark, date):
    playout_df = load_data_frame(spark, f"{PLAY_OUT_LOG_INPUT_PATH}/{date}", 'csv', True)\
        .withColumn('date', F.lit(date)) \
        .withColumn('break_start', parse_timestamp('Start Date', 'Start Time')) \
        .withColumn('break_end', parse_timestamp('End Date', 'End Time')) \
        .where('break_start is not null and break_end is not null') \
        .withColumn('break_start_time_int', F.expr('cast(break_start as long)')) \
        .withColumn('break_end_time_int', F.expr('cast(break_end as long)'))\
        .selectExpr('date', '`Content ID` as content_id', 'break_start_time_int', 'break_end_time_int', '`Creative Path` as creative_path') \
        .withColumn('duration', F.expr('break_end_time_int-break_start_time_int'))\
        .where('duration > 0 and duration < 3600 and creative_path != "aston"')
    save_data_frame(playout_df, PIPELINE_BASE_PATH + '/label' + PLAYOUT_LOG_PATH_SUFFIX + f"/cd={date}")
    return playout_df


def load_break_info_from_playout_logs(spark, date):
    playout_df = playout_data_processing(spark, date)
    break_info_df = break_info_processing(playout_df, date)
    return break_info_df


def load_wv_data(spark, date):
    data_source = "watched_video"
    timestamp_col = "ts_occurred_ms"
    if not check_s3_path_exist(PIPELINE_BASE_PATH + f"/label/{data_source}/cd={date}"):
        watch_video_df = spark.sql(f'select * from {WV_TABLE} where cd = "{date}"') \
            .withColumn('timestamp', F.expr('coalesce(cast(from_unixtime(CAST(ts_occurred_ms/1000 as BIGINT)) as timestamp), timestamp) as timestamp'))\
            .select("timestamp", 'received_at', 'watch_time', 'content_id', 'dw_p_id', 'dw_d_id') \
            .withColumn('wv_end_timestamp', F.substring(F.col('timestamp'), 1, 19)) \
            .withColumn('wv_end_timestamp', F.expr('if(wv_end_timestamp <= received_at, wv_end_timestamp, received_at)')) \
            .withColumn('watch_time', F.expr('cast(watch_time as int)')) \
            .withColumn('wv_start_timestamp', F.from_unixtime(F.unix_timestamp(F.col('wv_end_timestamp'), 'yyyy-MM-dd HH:mm:ss') - F.col('watch_time'))) \
            .withColumn('wv_start_timestamp', F.from_utc_timestamp(F.col('wv_start_timestamp'), "IST")) \
            .withColumn('wv_end_timestamp', F.from_utc_timestamp(F.col('wv_end_timestamp'), "IST")) \
            .withColumn('wv_start_time_int',
                        F.expr('cast(unix_timestamp(wv_start_timestamp, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .withColumn('wv_end_time_int',
                        F.expr('cast(unix_timestamp(wv_end_timestamp, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .drop('received_at', 'timestamp', 'wv_start_timestamp', 'wv_end_timestamp') \
            .cache()
        save_data_frame(watch_video_df, PIPELINE_BASE_PATH + f"/label/{data_source}/cd={date}")
    watch_video_df = load_data_frame(spark, PIPELINE_BASE_PATH + f"/label/{data_source}/cd={date}") \
        .cache()
    return watch_video_df


# calculate midroll inventory and reach ground truth
def calculate_midroll_inventory_and_reach_gt(spark, date):
    break_info_df = load_break_info_from_playout_logs(spark, date)
    watch_video_df = load_wv_data(spark, date)
    # calculate inventory and reach, need to extract the common intervals for each users
    total_inventory_df = watch_video_df\
        .join(F.broadcast(break_info_df), ['content_id'])\
        .withColumn('valid_duration', F.expr('least(wv_end_time_int, break_end_time_int) - greatest(wv_start_time_int, break_start_time_int)'))\
        .where('valid_duration > 0')\
        .groupBy('content_id')\
        .agg(F.sum('valid_duration').alias('total_duration'),
             F.countDistinct("dw_d_id").alias('total_reach'))\
        .withColumn('total_inventory', F.expr(f'cast((total_duration / 10) as bigint)')) \
        .withColumn('total_reach', F.expr(f'cast(total_reach as bigint)'))\
        .cache()
    save_data_frame(total_inventory_df, PIPELINE_BASE_PATH + f"/label/inventory/cd={date}")
    return total_inventory_df


# calculate preroll inventory and reach ground truth
def calculate_preroll_inventory_gt(spark, date):
    # calculate preroll sub & free inventory
    inventory_df = load_data_frame(spark, f"{AGGR_SHIFU_INVENTORY_PATH}/cd={date}")\
        .withColumn('user_account_type', F.upper(F.col('user_account_type')))\
        .where('upper(ad_placement) = "PREROLL"')\
        .withColumn('sub_tag', F.locate('SUBSCRIBED_FREE', F.col('user_account_type')))\
        .withColumn('sub_tag', F.expr('if(sub_tag>0 or user_account_type="", 0, 1)'))\
        .groupBy('content_id', 'sub_tag')\
        .agg(F.sum('inventory').alias('inventory'))\
        .cache()
    final_inventory_df = inventory_df\
        .where('sub_tag=0')\
        .selectExpr('content_id', 'inventory as preroll_free_inventory')\
        .join(inventory_df.where('sub_tag=1').selectExpr('content_id', 'inventory as preroll_sub_inventory'), 'content_id')
    save_data_frame(final_inventory_df, PIPELINE_BASE_PATH + f"/label/preroll_inventory/cd={date}")
    return final_inventory_df


# add wv related labels and inventory & reach labels
def add_labels_to_new_matches(spark, date, new_match_df):
    match_sub_df, match_free_df = calculate_reach_and_wt_from_wv_table(spark, date)
    midroll_inventory_df = calculate_midroll_inventory_and_reach_gt(spark, date)
    preroll_inventory_df = calculate_preroll_inventory_gt(spark, date)
    res_df = new_match_df\
        .drop("match_active_free_num", "watch_time_per_free_per_match",
              "match_active_sub_num", "watch_time_per_subscriber_per_match",
              "total_reach", "total_inventory", "preroll_sub_inventory", "preroll_free_inventory")\
        .join(match_sub_df, 'content_id')\
        .join(match_free_df, 'content_id') \
        .join(midroll_inventory_df, 'content_id')\
        .join(preroll_inventory_df, 'content_id') \
        .withColumn('preroll_free_sessions', F.expr('preroll_free_inventory/match_active_free_num')) \
        .withColumn('preroll_sub_sessions', F.expr('preroll_sub_inventory/match_active_sub_num'))
    return res_df


strip_udf = F.udf(lambda x: x.strip(), StringType())
