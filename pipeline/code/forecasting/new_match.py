from path import *
from util import *


def get_wt_data(spark, DATE):
    match_sub_df = load_data_frame(spark, f'{watchAggregatedInputPath}/cd={DATE}', fmt="orc") \
        .withColumn('subscription_status', F.upper(F.col('subscription_status'))) \
        .where(f'subscription_status in ("ACTIVE", "CANCELLED", "GRACEPERIOD")') \
        .groupBy('dw_p_id', 'content_id') \
        .agg(F.sum('watch_time').alias('watch_time')) \
        .groupBy('content_id') \
        .agg(F.countDistinct('dw_p_id').alias('match_active_sub_num'),
             F.sum('watch_time').alias('total_watch_time')) \
        .withColumn('watch_time_per_subscriber_per_match', F.expr('total_watch_time/match_active_sub_num')) \
        .select('content_id', 'match_active_sub_num', 'watch_time_per_subscriber_per_match') \
        .cache()
    match_free_df = load_data_frame(spark, f'{watchAggregatedInputPath}/cd={DATE}', fmt="orc") \
        .withColumn('subscription_status', F.upper(F.col('subscription_status'))) \
        .where(f'subscription_status not in ("ACTIVE", "CANCELLED", "GRACEPERIOD")') \
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
def save_break_list_info(playout_df, DATE):
    cols = ['content_id', 'start_time', 'end_time', 'delivered_duration']
    # fiter = 1 means selecting one from one break regardless of its source stream
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
    save_data_frame(res_df, pipeline_base_path + f"/label/break_info/cd={DATE}")


def reformat_playout_df(playout_df):
    return playout_df\
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
        .withColumnRenamed(creative_path_col, 'creative_path') \
        .withColumn('content_id', F.trim(F.col('content_id'))) \
        .withColumn('content_language', F.expr('lower(content_language)')) \
        .withColumn('platform', F.expr('lower(platform)')) \
        .withColumn('tenant', F.expr('lower(tenant)')) \
        .withColumn('creative_id', F.expr('upper(creative_id)')) \
        .withColumn('break_id', F.expr('upper(break_id)'))\
        .withColumn('creative_path', F.expr('lower(creative_path)')) \
        .withColumn('start_time', strip_udf('start_time')) \
        .withColumn('end_time', strip_udf('end_time'))


def save_playout_data(spark, DATE):
    playout_df = reformat_playout_df(load_data_frame(spark, f"{play_out_log_input_path}{DATE}", 'csv', True))\
        .withColumn('date', F.lit(DATE))\
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
        .where('duration > 0')
    save_data_frame(playout_df, pipeline_base_path + '/label' + playout_log_path_suffix + f"/cd={DATE}")


def get_inventory_data(spark, DATE):
    save_playout_data(spark, DATE)
    playout_df = load_data_frame(spark, pipeline_base_path + playout_log_path_suffix + f"/cd={DATE}") \
        .where('creative_path != "aston"') \
        .cache()
    data_source = "watched_video"
    timestamp_col = "ts_occurred_ms"
    if not check_s3_path_exist(pipeline_base_path+f"/label/{data_source}/cd={DATE}"):
        watch_video_df = load_data_frame(spark, f"{watch_video_path}/cd={DATE}") \
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
        save_data_frame(watch_video_df, pipeline_base_path+f"/label/{data_source}/cd={DATE}")
    else:
        watch_video_df = load_data_frame(spark, pipeline_base_path+f"/label/{data_source}/cd={DATE}")\
            .cache()
    save_break_list_info(playout_df, DATE)
    final_playout_df = load_data_frame(spark, pipeline_base_path + f"/label/break_info/cd={DATE}") \
        .withColumn('break_start_time_int', F.expr('cast(unix_timestamp(start_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
        .withColumn('break_end_time_int', F.expr('cast(unix_timestamp(end_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
        .withColumn('duration', F.expr('break_end_time_int-break_start_time_int'))\
        .where('duration > 0 and duration < 3600')\
        .cache()
    print(final_playout_df.count())
    # calculate inventory and reach, need to extract the common intervals for each users
    total_inventory_df = watch_video_df\
        .join(F.broadcast(final_playout_df), ['content_id'])\
        .withColumn('big_start_time', F.expr('if(wv_start_time_int < break_start_time_int, break_start_time_int, wv_start_time_int)'))\
        .withColumn('small_end_time', F.expr('if(wv_end_time_int < break_end_time_int, wv_end_time_int, break_end_time_int)'))\
        .withColumn('valid_duration', F.expr('small_end_time - big_start_time'))\
        .where('valid_duration > 0')\
        .groupBy('date', 'content_id')\
        .agg(F.sum('valid_duration').alias('total_duration'),
             F.countDistinct("dw_d_id").alias('total_reach'))\
        .withColumn('total_inventory', F.expr(f'cast((total_duration / 10) as bigint)')) \
        .withColumn('total_reach', F.expr(f'cast(total_reach as bigint)'))\
        .cache()
    save_data_frame(total_inventory_df, pipeline_base_path + f"/label/inventory/cd={DATE}")
    return total_inventory_df


def add_labels_to_new_matches(spark, DATE, new_match_df):
    match_sub_df, match_free_df = get_wt_data(spark, DATE)
    inventory_df = get_inventory_data(spark, DATE)
    res_df = new_match_df\
        .drop("match_active_free_num", "watch_time_per_free_per_match",
              "match_active_sub_num", "watch_time_per_subscriber_per_match",
              "total_reach", "total_inventory")\
        .join(match_sub_df, 'content_id')\
        .join(match_free_df, 'content_id') \
        .join(inventory_df, 'content_id')
    return res_df


strip_udf = F.udf(lambda x: x.strip(), StringType())
