from path import *
from util import *


# get wt related labels
def get_wt_data(spark, date):
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


# get break list of a specific content with break_start_time, break_end_time
def save_break_list_info(playout_df, date):
    cols = ['content_id', 'start_time', 'end_time', 'delivered_duration']
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
    save_data_frame(res_df, PIPELINE_BASE_PATH + f"/label/break_info/cd={date}")


# reformat playout logs
def reformat_playout_df(playout_df):
    return playout_df\
        .withColumnRenamed(CONTENT_ID_COL2, 'content_id')\
        .withColumnRenamed(START_TIME_COL2, 'start_time')\
        .withColumnRenamed(END_TIME_COL2, 'end_time')\
        .withColumnRenamed(BREAK_DURATION_COL2, 'delivered_duration')\
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
        .withColumnRenamed(BREAK_DURATION_COL, 'delivered_duration')\
        .withColumnRenamed(PLATFORM_COL, 'platform')\
        .withColumnRenamed(TENANT_COL, 'tenant')\
        .withColumnRenamed(CONTENT_LANGUAGE_COL, 'content_language')\
        .withColumnRenamed(CREATIVE_ID_COL, 'creative_id')\
        .withColumnRenamed(BREAK_ID_COL, 'break_id')\
        .withColumnRenamed(PLAYOUT_ID_COL, 'playout_id')\
        .withColumnRenamed(CREATIVE_PATH_COL, 'creative_path') \
        .withColumn('content_id', F.trim(F.col('content_id'))) \
        .withColumn('content_language', F.expr('lower(content_language)')) \
        .withColumn('platform', F.expr('lower(platform)')) \
        .withColumn('tenant', F.expr('lower(tenant)')) \
        .withColumn('creative_id', F.expr('upper(creative_id)')) \
        .withColumn('break_id', F.expr('upper(break_id)'))\
        .withColumn('creative_path', F.expr('lower(creative_path)')) \
        .withColumn('start_time', strip_udf('start_time')) \
        .withColumn('end_time', strip_udf('end_time'))


# playout data processing
def playout_data_processing(spark, date):
    playout_df = reformat_playout_df(load_data_frame(spark, f"{PLAY_OUT_LOG_INPUT_PATH}{date}", 'csv', True))\
        .withColumn('date', F.lit(date))\
        .select('date', 'content_id', 'start_time', 'end_time', 'delivered_duration',
                'platform', 'tenant', 'content_language', 'creative_id', 'break_id',
                'playout_id', 'creative_path') \
        .withColumn('start_time', F.expr('if(length(start_time)==8, start_time, from_unixtime(unix_timestamp(start_time, "hh:mm:ss aa"), "HH:mm:ss"))')) \
        .withColumn('end_time', F.expr('if(length(end_time)==8, end_time, from_unixtime(unix_timestamp(end_time, "hh:mm:ss aa"), "HH:mm:ss"))')) \
        .withColumn('delivered_duration', F.expr('cast(unix_timestamp(delivered_duration, "HH:mm:ss") as long)'))\
        .where('start_time is not null and end_time is not null')\
        .withColumn('simple_start_time', F.expr('substring(start_time, 1, 5)'))\
        .withColumn('start_time', F.concat_ws(" ", F.col('start_date'), F.col('start_time')))\
        .withColumn('end_time', F.concat_ws(" ", F.col('end_date'), F.col('end_time'))) \
        .withColumn('start_time_int', F.expr('cast(unix_timestamp(start_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
        .withColumn('end_time_int', F.expr('cast(unix_timestamp(end_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
        .withColumn('duration', F.expr('end_time_int-start_time_int'))\
        .where('duration > 0 and creative_path != "aston"')
    save_data_frame(playout_df, PIPELINE_BASE_PATH + '/label' + PLAYOUT_LOG_PATH_SUFFIX + f"/cd={date}")


# get inventory and reach
def get_inventory_data(spark, date):
    playout_data_processing(spark, date)
    playout_df = load_data_frame(spark, PIPELINE_BASE_PATH + PLAYOUT_LOG_PATH_SUFFIX + f"/cd={date}") \
        .cache()
    save_break_list_info(playout_df, date)
    final_playout_df = load_data_frame(spark, PIPELINE_BASE_PATH + f"/label/break_info/cd={date}") \
        .withColumn('break_start_time_int', F.expr('cast(unix_timestamp(start_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
        .withColumn('break_end_time_int', F.expr('cast(unix_timestamp(end_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
        .withColumn('duration', F.expr('break_end_time_int-break_start_time_int')) \
        .where('duration > 0 and duration < 3600') \
        .cache()
    print(final_playout_df.count())

    data_source = "watched_video"
    timestamp_col = "ts_occurred_ms"
    if not check_s3_path_exist(PIPELINE_BASE_PATH+f"/label/{data_source}/cd={date}"):
        watch_video_df = load_data_frame(spark, f"{WATCH_VIDEO_PATH}/cd={date}") \
            .withColumn("timestamp", F.expr(f'if(timestamp is null and {timestamp_col} is not null, '
                               f'from_unixtime({timestamp_col}/1000), timestamp)'))\
            .select("timestamp", 'received_at', 'watch_time', 'content_id', 'dw_p_id',
                    'dw_d_id')\
            .withColumn('wv_end_timestamp', F.substring(F.col('timestamp'), 1, 19)) \
            .withColumn('wv_end_timestamp', F.expr('if(wv_end_timestamp <= received_at, wv_end_timestamp, received_at)')) \
            .withColumn('watch_time', F.expr('cast(watch_time as int)')) \
            .withColumn('wv_start_timestamp', F.from_unixtime(F.unix_timestamp(F.col('wv_end_timestamp'), 'yyyy-MM-dd HH:mm:ss') - F.col('watch_time'))) \
            .withColumn('wv_start_timestamp', F.from_utc_timestamp(F.col('wv_start_timestamp'), "IST")) \
            .withColumn('wv_end_timestamp', F.from_utc_timestamp(F.col('wv_end_timestamp'), "IST")) \
            .withColumn('wv_start_time_int', F.expr('cast(unix_timestamp(wv_start_timestamp, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .withColumn('wv_end_time_int', F.expr('cast(unix_timestamp(wv_end_timestamp, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .drop('received_at', 'timestamp', 'wv_start_timestamp', 'wv_end_timestamp')\
            .cache()
        save_data_frame(watch_video_df, PIPELINE_BASE_PATH+f"/label/{data_source}/cd={date}")
    else:
        watch_video_df = load_data_frame(spark, PIPELINE_BASE_PATH+f"/label/{data_source}/cd={date}")\
            .cache()

    # calculate inventory and reach, need to extract the common intervals for each users
    total_inventory_df = watch_video_df\
        .join(F.broadcast(final_playout_df), ['content_id'])\
        .withColumn('big_start_time', F.expr('if(wv_start_time_int < break_start_time_int, break_start_time_int, wv_start_time_int)'))\
        .withColumn('small_end_time', F.expr('if(wv_end_time_int < break_end_time_int, wv_end_time_int, break_end_time_int)'))\
        .withColumn('valid_duration', F.expr('small_end_time - big_start_time'))\
        .where('valid_duration > 0')\
        .groupBy('content_id')\
        .agg(F.sum('valid_duration').alias('total_duration'),
             F.countDistinct("dw_d_id").alias('total_reach'))\
        .withColumn('total_inventory', F.expr(f'cast((total_duration / 10) as bigint)')) \
        .withColumn('total_reach', F.expr(f'cast(total_reach as bigint)'))\
        .cache()
    save_data_frame(total_inventory_df, PIPELINE_BASE_PATH + f"/label/inventory/cd={date}")
    return total_inventory_df


# add wt related and inventory labels
def add_labels_to_new_matches(spark, date, new_match_df):
    match_sub_df, match_free_df = get_wt_data(spark, date)
    inventory_df = get_inventory_data(spark, date)
    res_df = new_match_df\
        .drop("match_active_free_num", "watch_time_per_free_per_match",
              "match_active_sub_num", "watch_time_per_subscriber_per_match",
              "total_reach", "total_inventory")\
        .join(match_sub_df, 'content_id')\
        .join(match_free_df, 'content_id') \
        .join(inventory_df, 'content_id')
    return res_df


strip_udf = F.udf(lambda x: x.strip(), StringType())
