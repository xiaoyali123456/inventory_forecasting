import sys

import pyspark.sql.functions as F

from path import *
from util import *
from config import *


# load prediction dataset
def load_prediction_dataset(run_date):
    if run_date >= "2023-09-11":
        factor = 1.0
    else:
        factor = 1.3
    prediction_feature_df = load_data_frame(spark, PREDICTION_MATCH_TABLE_PATH + f"/cd={run_date}")\
        .selectExpr('requestId as request_id', 'matchId as match_id', 'content_id', 'date', 'tournament', 'teams', 'vod_type',
                    'total_frees_number', 'total_subscribers_number', 'match_duration', 'break_duration')\
        .withColumn('total_frees_number', F.expr(f"total_frees_number * {factor}"))\
        .withColumn('total_subscribers_number', F.expr(f"total_subscribers_number * {factor}"))\
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
        .withColumn('estimated_reach', F.expr(f"(estimated_free_match_number + estimated_sub_match_number)  * {RETENTION_RATE}")) \
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
    res = []
    the_day_before_run_date = get_date_list(run_date, -2)[0]
    last_update_date = get_last_cd(TOTAL_INVENTORY_PREDICTION_PATH, end=run_date)
    print(the_day_before_run_date)
    print(last_update_date)
    gt_dau_df = load_data_frame(spark, f'{DAU_TRUTH_PATH}cd={run_date}/')\
        .withColumnRenamed('ds', 'date')\
        .where(f'date="{the_day_before_run_date}"')\
        .cache()
    gt_inv_df = load_data_frame(spark, f'{TRAIN_MATCH_TABLE_PATH}/cd={run_date}/') \
        .where(f'date="{the_day_before_run_date}"') \
        .selectExpr('date', 'teams', *LABEL_COLS) \
        .withColumn('teams', F.concat_ws(' vs ', F.col('teams'))) \
        .withColumn('overall_vv', F.expr('match_active_free_num+match_active_sub_num')) \
        .withColumn('avod_wt', F.expr('match_active_free_num*watch_time_per_free_per_match')) \
        .withColumn('svod_wt', F.expr('match_active_sub_num*watch_time_per_subscriber_per_match')) \
        .withColumn('overall_wt', F.expr('avod_wt+svod_wt')) \
        .withColumn('avod_reach',
                    F.expr('total_reach*(match_active_free_num/(match_active_free_num+match_active_sub_num))')) \
        .withColumn('svod_reach',
                    F.expr('total_reach*(match_active_sub_num/(match_active_free_num+match_active_sub_num))')) \
        .join(gt_dau_df, 'date') \
        .selectExpr('date', 'teams', 'vv as overall_dau', 'free_vv as avod_dau', 'sub_vv as svod_dau',
                    'overall_vv', 'match_active_free_num as avod_vv', 'match_active_sub_num as svod_vv', 'match_active_free_num/match_active_sub_num as vv_rate',
                    'overall_wt', 'avod_wt', 'svod_wt', 'total_inventory',
                    'total_reach', 'avod_reach', 'svod_reach') \
        .cache()
    cols = gt_inv_df.columns[2:]
    for col in cols:
        if col != "vv_rate":
            gt_inv_df = gt_inv_df.withColumn(col, F.expr(f'round({col} / 1000000.0, 1)'))
        else:
            gt_inv_df = gt_inv_df.withColumn(col, F.expr(f'round({col}, 1)'))
    # publish_to_slack(topic=SLACK_NOTIFICATION_TOPIC, title="ground truth of matches", output_df=gt_inv_df, region=REGION)
    res.append(gt_inv_df.withColumn('tag', F.lit('gt')))
    factor = 1.3
    predict_dau_df = load_data_frame(spark, f'{DAU_FORECAST_PATH}cd={last_update_date}/') \
        .withColumnRenamed('ds', 'date') \
        .withColumn('vv', F.expr(f"vv * {factor}")) \
        .withColumn('free_vv', F.expr(f"free_vv * {factor}")) \
        .withColumn('sub_vv', F.expr(f"vv - free_vv")) \
        .cache()
    predict_inv_df = load_data_frame(spark, f'{TOTAL_INVENTORY_PREDICTION_PATH}/cd={last_update_date}/') \
        .where(f'date="{the_day_before_run_date}"') \
        .withColumn('avod_vv', F.expr('estimated_free_match_number/1')) \
        .withColumn('avod_reach', F.expr(f'estimated_free_match_number * {RETENTION_RATE}')) \
        .withColumn('svod_vv', F.expr('estimated_sub_match_number/1')) \
        .withColumn('svod_reach', F.expr(f'estimated_sub_match_number  * {RETENTION_RATE}')) \
        .withColumn('overall_vv', F.expr('avod_vv+svod_vv')) \
        .withColumn('avod_wt', F.expr('estimated_free_match_number * estimated_watch_time_per_free_per_match')) \
        .withColumn('svod_wt', F.expr('estimated_sub_match_number * estimated_watch_time_per_subscriber_per_match')) \
        .withColumn('overall_wt', F.expr('avod_wt+svod_wt')) \
        .join(predict_dau_df, 'date') \
        .selectExpr('date', 'teams', 'vv as overall_dau', 'free_vv as avod_dau', 'sub_vv as svod_dau',
                    'overall_vv', 'avod_vv', 'svod_vv', 'avod_vv/svod_vv as vv_rate',
                    'overall_wt', 'avod_wt', 'svod_wt', 'estimated_inventory as total_inventory',
                    f'estimated_reach as total_reach', 'avod_reach', 'svod_reach')
    teams = predict_inv_df.select('teams').collect()[0][0]
    cols = predict_inv_df.columns[2:]
    if last_update_date >= "2023-09-11":
        for col in cols[3:]:
            if col != "vv_rate":
                predict_inv_df = predict_inv_df.withColumn(col, F.expr(f'round({col} * {factor}, 1)'))
            else:
                predict_inv_df = predict_inv_df.withColumn(col, F.expr(f'round({col}, 1)'))
    for col in cols:
        if col != "vv_rate":
            predict_inv_df = predict_inv_df.withColumn(col, F.expr(f'round({col} / 1000000.0, 1)'))
        else:
            predict_inv_df = predict_inv_df.withColumn(col, F.expr(f'round({col}, 1)'))
    # publish_to_slack(topic=SLACK_NOTIFICATION_TOPIC, title="prediction of matches with multiple 1.3", output_df=predict_inv_df, region=REGION)
    res.append(predict_inv_df.withColumn('tag', F.lit('ml_dynamic_model_with_factor_1.3')))
    cols = predict_inv_df.columns[2:]
    for col in cols:
        if col != "vv_rate":
            predict_inv_df = predict_inv_df.withColumn(col, F.expr(f'round({col} / {factor}, 1)'))
        else:
            predict_inv_df = predict_inv_df.withColumn(col, F.expr(f'round({col}, 1)'))
    # publish_to_slack(topic=SLACK_NOTIFICATION_TOPIC, title="prediction of matches without multiple 1.3", output_df=predict_inv_df, region=REGION)
    res.append(predict_inv_df.withColumn('tag', F.lit('ml_dynamic_model')))
    growth_df = load_data_frame(spark, GROWTH_PREDICITON_PATH, "csv", True).where(f"date='{the_day_before_run_date}'").cache()
    growth_cols = growth_df.columns
    for col in cols:
        if col == "vv_rate":
            growth_df = growth_df.withColumn(col, F.expr(f"round(avod_vv/svod_vv, 1)"))
        else:
            if col in growth_cols:
                growth_df = growth_df.withColumn(col, F.expr(f"cast({col} as float)"))
                growth_df = growth_df.withColumn(col, F.expr(f"round({col}/1.0, 1)"))
            else:
                growth_df = growth_df.withColumn(col, F.lit(None))
    res.append(growth_df.select(predict_inv_df.columns).withColumn('tag', F.lit('growth_team_method')))
    if the_day_before_run_date > "2023-09-05":
        predict_inv_df = load_data_frame(spark, ML_STATIC_MODEL_PREDICITON_PATH, "csv", True).where(f"date='{the_day_before_run_date}'").cache()
        cols = predict_inv_df.columns[2:]
        for col in cols:
            predict_inv_df = predict_inv_df.withColumn(col, F.expr(f"cast({col} as float)"))
        predict_inv_df = predict_inv_df\
            .withColumnRenamed('free_vv', 'avod_vv')\
            .withColumnRenamed('sub_vv', 'svod_vv') \
            .withColumn('avod_dau', F.expr(f"avod_dau * {factor}")) \
            .withColumn('svod_dau', F.expr(f"svod_dau * {factor}")) \
            .selectExpr('date', 'teams', 'avod_dau+svod_dau as overall_dau', 'avod_dau', 'svod_dau',
                        'estimated_vv as overall_vv', 'avod_vv', 'svod_vv', 'avod_vv/svod_vv as vv_rate',
                        'estimated_watch_time_per_free_per_match*avod_vv+estimated_watch_time_per_subscriber_per_match*svod_vv as overall_wt',
                        'estimated_watch_time_per_free_per_match*avod_vv as avod_wt', 'estimated_watch_time_per_subscriber_per_match*svod_vv as svod_wt',
                        'estimated_inventory as total_inventory',
                        f'estimated_reach as total_reach', 'free_reach as avod_reach', 'sub_reach as svod_reach')
    else:
        base_date = "2023-08-21"
        predict_dau_df = load_data_frame(spark, f'{DAU_FORECAST_PATH}cd={base_date}/') \
            .withColumnRenamed('ds', 'date') \
            .withColumn('vv', F.expr(f"vv * {factor}")) \
            .withColumn('free_vv', F.expr(f"free_vv * {factor}")) \
            .withColumn('sub_vv', F.expr(f"vv - free_vv")) \
            .cache()
        predict_inv_df = load_data_frame(spark, f'{TOTAL_INVENTORY_PREDICTION_PATH}/cd={base_date}/') \
            .where(f'date="{the_day_before_run_date}"') \
            .withColumn('avod_vv', F.expr('estimated_free_match_number/1')) \
            .withColumn('avod_reach', F.expr(f'estimated_free_match_number * {RETENTION_RATE}')) \
            .withColumn('svod_vv', F.expr('estimated_sub_match_number/1')) \
            .withColumn('svod_reach', F.expr(f'estimated_sub_match_number  * {RETENTION_RATE}')) \
            .withColumn('overall_vv', F.expr('avod_vv+svod_vv')) \
            .withColumn('avod_wt', F.expr('estimated_free_match_number * estimated_watch_time_per_free_per_match')) \
            .withColumn('svod_wt', F.expr('estimated_sub_match_number * estimated_watch_time_per_subscriber_per_match')) \
            .withColumn('overall_wt', F.expr('avod_wt+svod_wt')) \
            .join(predict_dau_df, 'date') \
            .selectExpr('date', 'teams', 'vv as overall_dau', 'free_vv as avod_dau', 'sub_vv as svod_dau',
                        'overall_vv', 'avod_vv', 'svod_vv', 'avod_vv/svod_vv as vv_rate',
                        'overall_wt', 'avod_wt', 'svod_wt', 'estimated_inventory as total_inventory',
                        f'estimated_reach as total_reach', 'avod_reach', 'svod_reach')
    cols = predict_inv_df.columns[2:]
    for col in cols:
        if col != "vv_rate":
            predict_inv_df = predict_inv_df.withColumn(col, F.expr(f'round({col} / 1000000.0, 1)'))
        else:
            predict_inv_df = predict_inv_df.withColumn(col, F.expr(f'round({col}, 1)'))
    res.append(predict_inv_df.withColumn('tag', F.lit('ml_static_model_with_factor_1.3')))
    for col in cols:
        if col != "vv_rate":
            predict_inv_df = predict_inv_df.withColumn(col, F.expr(f'round({col} / {factor}, 1)'))
        else:
            predict_inv_df = predict_inv_df.withColumn(col, F.expr(f'round({col}, 1)'))
    # publish_to_slack(topic=SLACK_NOTIFICATION_TOPIC, title="prediction of matches without multiple 1.3", output_df=predict_inv_df, region=REGION)
    res.append(predict_inv_df.withColumn('tag', F.lit('ml_static_model')))
    res_df = res[0].union(res[2]).union(res[1]).union(res[5]).union(res[4]).union(res[3])
    res_cols = res_df.columns
    for col in cols:
        res_df = res_df.withColumn(col, F.expr(f'cast({col} as double)'))
    publish_to_slack(topic=SLACK_NOTIFICATION_TOPIC, title="inventory prediction of matches",
                     output_df=res_df.withColumn('teams', F.expr(f'if(teams!="{teams}", "{teams}", teams)')).select(res_cols), region=REGION)
    save_data_frame(res_df.withColumn('teams', F.expr(f'if(teams!="{teams}", "{teams}", teams)')).select(res_cols), f"{METRICS_PATH}cd={the_day_before_run_date}")


# for run_date in get_date_list("2023-08-31", 12):
#     if check_s3_path_exist(f"{PREDICTION_MATCH_TABLE_PATH}/cd={run_date}/"):
#         print(run_date)
#         output_metrics_of_finished_matches(run_date)


if __name__ == '__main__':
    run_date = sys.argv[1]
    if check_s3_path_exist(f"{PREDICTION_MATCH_TABLE_PATH}/cd={run_date}/"):
        main(run_date)
        output_metrics_of_finished_matches(run_date)
        slack_notification(topic=SLACK_NOTIFICATION_TOPIC, region=REGION,
                           message=f"inventory forecasting on {run_date} is done.")
    else:
        slack_notification(topic=SLACK_NOTIFICATION_TOPIC, region=REGION,
                           message=f"inventory forecasting on {run_date} nothing update.")


