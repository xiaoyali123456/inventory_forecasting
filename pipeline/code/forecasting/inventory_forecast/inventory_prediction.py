import sys

import pyspark.sql.functions as F

from path import *
from util import *
from config import *


# load prediction dataset
def load_prediction_dataset(run_date):
    prediction_feature_df = load_data_frame(spark, PREDICTION_MATCH_TABLE_PATH + f"/cd={run_date}")\
        .selectExpr('requestId as request_id', 'matchId as match_id', 'content_id', 'date', 'tournament', 'teams', 'vod_type',
                    'total_frees_number', 'total_subscribers_number', 'match_duration', 'break_duration')\
        .withColumn('total_frees_number', F.expr("total_frees_number * 1.3"))\
        .withColumn('total_subscribers_number', F.expr("total_subscribers_number * 1.3"))\
        .withColumn('teams', F.concat_ws(" vs ", F.col('teams')))\
        .cache()
    return prediction_feature_df


# load dnn prediction results
def load_dnn_predictions(df, run_date):
    label_path = f"{PIPELINE_BASE_PATH}/dnn_predictions/cd={run_date}"
    common_cols = ['content_id']
    # load parameters predicted by dnn models
    return df \
        .join(load_data_frame(spark, f"{label_path}/label={FREE_RATE_LABEL}"), common_cols) \
        .join(load_data_frame(spark, f"{label_path}/label={FREE_WT_LABEL}"), common_cols) \
        .join(load_data_frame(spark, f"{label_path}/label={SUB_RATE_LABEL}"), common_cols) \
        .join(load_data_frame(spark, f"{label_path}/label={SUB_WT_LABEL}"), common_cols)\
        .join(load_data_frame(spark, f"{label_path}/label={PREROLL_SUB_SESSIONS}"), common_cols)\
        .join(load_data_frame(spark, f"{label_path}/label={PREROLL_FREE_SESSIONS}"), common_cols)


# Forecast inventory at match level
def main(run_date):
    prediction_feature_df = load_prediction_dataset(run_date)
    partition_col = "request_id"
    # load parameters predicted by dnn models
    prediction_feature_df = load_dnn_predictions(prediction_feature_df, run_date)
    res_df = prediction_feature_df \
        .withColumn('estimated_free_match_number', F.expr('total_frees_number * estimated_frees_watching_match_rate')) \
        .withColumn('estimated_sub_match_number', F.expr('total_subscribers_number * estimated_subscribers_watching_match_rate')) \
        .withColumn('estimated_avg_concurrency', F.expr('(estimated_free_match_number * estimated_watch_time_per_free_per_match '
        f'+ estimated_sub_match_number * estimated_watch_time_per_subscriber_per_match)/match_duration')) \
        .withColumn('estimated_inventory', F.expr(f'estimated_avg_concurrency * {RETENTION_RATE} * (break_duration / 10.0)')) \
        .withColumn('estimated_reach', F.expr(f"estimated_free_match_number + estimated_sub_match_number")) \
        .withColumn('estimated_inventory', F.expr('cast(estimated_inventory as bigint)')) \
        .withColumn('estimated_reach', F.expr('cast(estimated_reach as bigint)')) \
        .withColumn('estimated_preroll_free_inventory', F.expr(f'if(array_contains(vod_type, "avod"), '
                                                       f'estimated_free_match_number * estimated_{PREROLL_SUB_SESSIONS}, '
                                                       f'estimated_free_match_number * estimated_{PREROLL_FREE_SESSIONS})')) \
        .withColumn('estimated_preroll_sub_inventory', F.expr(f'estimated_sub_match_number * estimated_{PREROLL_SUB_SESSIONS}')) \
        .withColumn('estimated_preroll_inventory', F.expr('estimated_preroll_free_inventory + estimated_preroll_sub_inventory')) \
        .withColumn('estimated_preroll_inventory', F.expr('cast(estimated_preroll_inventory as bigint)')) \
        .cache()
    res_df.orderBy('date').show(1000, False)
    res_df\
        .groupBy('tournament')\
        .agg(F.sum('estimated_inventory').alias('estimated_inventory'),
             F.sum('estimated_reach').alias('estimated_reach'),
             F.sum('estimated_preroll_inventory').alias('estimated_preroll_inventory'),
             F.count('content_id').alias('match_num'))\
        .show(1000, False)
    save_data_frame(res_df, TOTAL_INVENTORY_PREDICTION_PATH + f"cd={run_date}/")


def output_metrics_of_finished_matches(run_date):
    the_day_before_run_date = get_date_list(run_date, -2)[0]
    gt_dau_df = load_data_frame(spark, f'{DAU_TRUTH_PATH}cd={run_date}/').withColumnRenamed('ds', 'date').cache()
    gt_inv_df = load_data_frame(spark, f'{TRAIN_MATCH_TABLE_PATH}/cd={run_date}/') \
        .where(f'date="{the_day_before_run_date}"') \
        .selectExpr('date', 'content_id', *LABEL_COLS) \
        .withColumn('overall_vv', F.expr('match_active_free_num+match_active_sub_num')) \
        .withColumn('avod_wt', F.expr('match_active_free_num*watch_time_per_free_per_match')) \
        .withColumn('svod_wt', F.expr('match_active_sub_num*watch_time_per_subscriber_per_match')) \
        .withColumn('overall_wt', F.expr('avod_wt+svod_wt')) \
        .withColumn('avod_reach',
                    F.expr('total_reach*(match_active_free_num/(match_active_free_num+match_active_sub_num))')) \
        .withColumn('svod_reach',
                    F.expr('total_reach*(match_active_sub_num/(match_active_free_num+match_active_sub_num))')) \
        .join(gt_dau_df, 'date') \
        .selectExpr('date', 'content_id', 'vv as overall_dau', 'free_vv as avod_dau', 'sub_vv as svod_dau',
                    'overall_vv', 'match_active_free_num as avod_vv', 'match_active_sub_num as svod_vv',
                    'overall_wt', 'avod_wt', 'svod_wt', 'total_inventory',
                    'total_reach', 'avod_reach', 'svod_reach') \
        .cache()
    cols = gt_inv_df.columns[2:]
    for col in cols:
        gt_inv_df = gt_inv_df.withColumn(col, F.expr(f'{col} / 1000000.0'))
    publish_to_slack(topic=SLACK_NOTIFICATION_TOPIC, title="ground truth of matches ", output_df=gt_inv_df, region=REGION)


if __name__ == '__main__':
    run_date = sys.argv[1]
    if check_s3_path_exist(f"{PREDICTION_MATCH_TABLE_PATH}/cd={run_date}/"):
        main(run_date)
        slack_notification(topic=SLACK_NOTIFICATION_TOPIC, region=REGION,
                           message=f"inventory forecasting on {run_date} is done.")
    else:
        slack_notification(topic=SLACK_NOTIFICATION_TOPIC, region=REGION,
                           message=f"inventory forecasting on {run_date} nothing update.")


