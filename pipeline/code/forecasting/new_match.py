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


def get_inventory_data(spark, DATE):
    pass


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
