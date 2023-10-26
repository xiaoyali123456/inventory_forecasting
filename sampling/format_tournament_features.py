import sys

import pyspark.sql.functions as F
from pyspark.sql.types import *
from functools import reduce

from path import *
from util import *
from config import *

MODEL_VERSION = "_post_process"
TOTAL_INVENTORY_PREDICTION_PATH = f'{PIPELINE_BASE_PATH}/inventory_prediction{MODEL_VERSION}/future_tournaments/'
FINAL_INVENTORY_PREDICTION_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/final/inventory/'
FINAL_REACH_PREDICTION_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/final/reach/'
FINAL_ALL_PREDICTION_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/final/all/'
FINAL_ALL_PREDICTION_TOURNAMENT_PARTITION_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/final/all_tournament_partition/'
FINAL_ALL_PREROLL_PREDICTION_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/final/preroll_all/'
FINAL_ALL_PREROLL_PREDICTION_TOURNAMENT_PARTITION_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/final/preroll_all_tournament_partition/'
GROWTH_PREDICITON_PATH = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/pipeline/label/growth_prediction/"
ML_STATIC_MODEL_PREDICITON_FOR_AC2023_PATH = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/pipeline/label/ml_static_model_prediction/"
ML_STATIC_MODEL_PREDICITON_FOR_CWC2023_PATH = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/pipeline/label/ml_static_model_prediction_for_cwc2023/"
METRICS_PATH = f"s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/pipeline/label/metrics{MODEL_VERSION}"


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
    label_path = f"{PIPELINE_BASE_PATH}/dnn_predictions{MODEL_VERSION}/cd={run_date}"
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
        .withColumn('estimated_reach', F.expr('cast(estimated_reach as bigint)'))
    # res_df\
    #     .groupBy('tournament')\
    #     .agg(F.sum('estimated_inventory').alias('estimated_inventory'),
    #          F.sum('estimated_reach').alias('estimated_reach'),
    #          F.sum('estimated_preroll_inventory').alias('estimated_preroll_inventory'),
    #          F.count('content_id').alias('match_num'))\
    #     .show(1000, False)
    save_data_frame(res_df, TOTAL_INVENTORY_PREDICTION_PATH + f"cd={run_date}/")


@F.udf(returnType=StringType())
def unify_teams(teams):
    teams = teams.lower().split(" vs ")
    team1 = teams[0]
    team2 = teams[1]
    for key in SHORT_TEAM_MAPPING:
        if SHORT_TEAM_MAPPING[key] == team1:
            teams[0] = key
        if SHORT_TEAM_MAPPING[key] == team2:
            teams[1] = key
    return " vs ".join(sorted(teams))


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
    if gt_inv_df.count() == 0:
        return
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
        .withColumn('sub_vv', F.expr(f"sub_vv * {factor}")) \
        .withColumn('vv', F.expr(f"free_vv + sub_vv")) \
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
    if "2023-09-05" < the_day_before_run_date:
        predict_inv_df = load_data_frame(spark, ML_STATIC_MODEL_PREDICITON_FOR_CWC2023_PATH, "csv", True).where(f"date='{the_day_before_run_date}'").cache()
        # predict_inv_df.printSchema()
        # predict_inv_df.show(2)
        cols = predict_inv_df.columns[2:]
        for col in cols:
            predict_inv_df = predict_inv_df.withColumn(col, F.expr(f"cast({col} as float)"))
        predict_inv_df = predict_inv_df\
            .selectExpr('date', 'teams', 'overall_dau', 'avod_dau', 'svod_dau',
                        'overall_vv', 'avod_vv', 'svod_vv', 'vv_rate',
                        'overall_wt',
                        'avod_wt', 'svod_wt',
                        'total_inventory',
                        f'total_reach', 'avod_reach', 'svod_reach')
    else:
        base_date = "2023-08-21"
        # base_date = "2023-09-18"
        predict_dau_df = load_data_frame(spark, f'{DAU_FORECAST_PATH}cd={base_date}/') \
            .withColumnRenamed('ds', 'date') \
            .withColumn('vv', F.expr(f"free_vv + sub_vv")) \
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
            predict_inv_df = predict_inv_df.withColumn(col, F.expr(f'round({col} / 1.0, 1)'))
        else:
            predict_inv_df = predict_inv_df.withColumn(col, F.expr(f'round({col}, 1)'))
    res.append(predict_inv_df.withColumn('tag', F.lit('ml_static_model')))
    for col in cols:
        if col != "vv_rate":
            predict_inv_df = predict_inv_df.withColumn(col, F.expr(f'round({col} * {factor}, 1)'))
        else:
            predict_inv_df = predict_inv_df.withColumn(col, F.expr(f'round({col}, 1)'))
    # publish_to_slack(topic=SLACK_NOTIFICATION_TOPIC, title="prediction of matches without multiple 1.3", output_df=predict_inv_df, region=REGION)
    res.append(predict_inv_df.withColumn('tag', F.lit('ml_static_model_with_factor_1.3')))
    res_df = res[0].union(res[2]).union(res[1]).union(res[4]).union(res[5]).union(res[3])
    res_cols = res_df.columns
    for col in cols:
        res_df = res_df.withColumn(col, F.expr(f'cast({col} as double)'))
    res_df = res_df.withColumn('teams', unify_teams('teams'))
    # publish_to_slack(topic=SLACK_NOTIFICATION_TOPIC, title="inventory prediction of matches",
    #                  output_df=res_df.select(res_cols), region=REGION)
    save_data_frame(res_df.select(res_cols), f"{METRICS_PATH}/cd={the_day_before_run_date}")


def output_metrics(last_update_date):
    print(last_update_date)
    factor = 1.0
    predict_dau_df = load_data_frame(spark, f'{DAU_FORECAST_PATH}cd={last_update_date}/') \
        .withColumnRenamed('ds', 'date') \
        .withColumn('vv', F.expr(f"vv * {factor}")) \
        .withColumn('free_vv', F.expr(f"free_vv * {factor}")) \
        .withColumn('sub_vv', F.expr(f"sub_vv * {factor}")) \
        .withColumn('vv', F.expr(f"free_vv + sub_vv")) \
        .cache()
    predict_inv_df = load_data_frame(spark, f'{TOTAL_INVENTORY_PREDICTION_PATH}/cd={last_update_date}/') \
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


def check_inventory_changes(run_date):
    last_update_date = get_last_cd(TOTAL_INVENTORY_PREDICTION_PATH, end=run_date)
    old_df = load_data_frame(spark, f'{TOTAL_INVENTORY_PREDICTION_PATH}/cd={last_update_date}/')\
        .selectExpr('date', 'content_id', 'teams', 'estimated_inventory as old_estimated_inventory')
    new_df = load_data_frame(spark, f'{TOTAL_INVENTORY_PREDICTION_PATH}/cd={run_date}/') \
        .selectExpr('date', 'content_id', 'estimated_inventory as new_estimated_inventory')\
        .join(old_df, ['date', 'content_id'])\
        .withColumn('rate', F.expr('new_estimated_inventory/old_estimated_inventory'))\
        .where('rate > 2 or rate < 0.5')
    new_df.show(100, False)
    if new_df.count() > 0:
        publish_to_slack(topic=SLACK_NOTIFICATION_TOPIC, title="ALERT: inventory change largely",
                         output_df=new_df, region=REGION)
        # slack_notification(topic=SLACK_NOTIFICATION_TOPIC, region=REGION,
        #                    message=f"ALERT: inventory change largely on {run_date}!")


def output_metrics_of_tournament(date_list, prediction_path):
    df = reduce(lambda x, y: x.union(y), [load_data_frame(spark, f"{prediction_path}/cd={date}") for date in date_list]) \
        .where('tag in ("ml_dynamic_model", "gt")') \
        .withColumn('if_contain_india_team', F.expr(f"if(locate('india', teams)=0, 0, 1)")).cache()
    # df.where('tag in ("ml_dynamic_model")').orderBy('date').show(200, False)
    res = df.groupby('tag').agg(F.sum('overall_wt').alias('overall_wt')).orderBy('tag')
    print(res.collect()[1][1], res.collect()[0][1], res.collect()[1][1] / res.collect()[0][1] - 1)
    res = df.groupby('if_contain_india_team', 'tag').agg(F.sum('overall_wt').alias('overall_wt')).orderBy(
        'if_contain_india_team', 'tag').cache()
    print("india")
    print(res.collect()[3][2], res.collect()[2][2], res.collect()[3][2] / res.collect()[2][2] - 1)
    print("non india")
    print(res.collect()[1][2], res.collect()[0][2], res.collect()[1][2] / res.collect()[0][2] - 1)


main("2023-09-30")
for run_date in get_date_list("2023-10-06", 20):
    if check_s3_path_exist(f"{PREDICTION_MATCH_TABLE_PATH}/cd={run_date}/"):
        print(run_date)
        main(run_date)

for run_date in get_date_list("2023-10-06", 20):
    if check_s3_path_exist(f"{PREDICTION_MATCH_TABLE_PATH}/cd={run_date}/"):
        print(run_date)
        output_metrics_of_finished_matches(run_date)

df = reduce(lambda x, y: x.union(y), [load_data_frame(spark, f"{METRICS_PATH}/cd={date}") for date in get_date_list("2023-10-05", 18)]) \
        .where('tag in ("ml_dynamic_model")').selectExpr('date', 'teams', 'overall_wt')
df2 = reduce(lambda x, y: x.union(y), [load_data_frame(spark, f"s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/pipeline/label/metrics/cd={date}") for date in get_date_list("2023-10-05", 18)]) \
        .where('tag in ("ml_dynamic_model")').selectExpr('date', 'teams', 'overall_wt as overall_wt_base')
df3 = reduce(lambda x, y: x.union(y), [load_data_frame(spark, f"s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/pipeline/label/metrics/cd={date}") for date in get_date_list("2023-10-05", 18)]) \
        .where('tag in ("gt")').selectExpr('date', 'teams', 'overall_wt as overall_wt_gt')
res = df.join(df2, ['date', 'teams']).join(df3, ['date', 'teams'])\
    .withColumn('new_abs_error', F.expr('abs(overall_wt/overall_wt_gt-1)'))\
    .withColumn('old_abs_error', F.expr('abs(overall_wt_base/overall_wt_gt-1)'))
res.withColumn('tag', F.lit('abs_erro')).groupby('tag').agg(F.avg('overall_wt'), F.avg('overall_wt_base'),F.avg('overall_wt_gt'), F.avg('new_abs_error'), F.avg('old_abs_error')).show(1000,False)
print(METRICS_PATH)
output_metrics_of_tournament(get_date_list("2023-10-05", 21), METRICS_PATH)


# load dnn prediction results
def load_dnn_predictions_one_date(run_date, MODEL_VERSION, content_id_df):
    last_update_date = get_last_cd(TOTAL_INVENTORY_PREDICTION_PATH, end=run_date)
    label_path = f"{PIPELINE_BASE_PATH}/dnn_predictions{MODEL_VERSION}/cd={last_update_date}"
    common_cols = ['content_id']
    # load parameters predicted by dnn models
    return load_data_frame(spark, f"{label_path}/label={FREE_RATE_LABEL}").join(content_id_df, 'content_id').where(f'date="{run_date}"') \
        .join(load_data_frame(spark, f"{label_path}/label={FREE_WT_LABEL}"), common_cols) \
        .join(load_data_frame(spark, f"{label_path}/label={SUB_RATE_LABEL}"), common_cols) \
        .join(load_data_frame(spark, f"{label_path}/label={SUB_WT_LABEL}"), common_cols)


content_id_df = load_data_frame(spark, f'{TRAIN_MATCH_TABLE_PATH}/cd=2023-10-25/').select('date', 'content_id').distinct().cache()
old_prediction_df = reduce(lambda x, y: x.union(y), [load_dnn_predictions_one_date(date, "", content_id_df) for date in get_date_list("2023-10-05", 18)])
new_prediction_df = reduce(lambda x, y: x.union(y), [load_dnn_predictions_one_date(date, MODEL_VERSION, content_id_df) for date in get_date_list("2023-10-05", 18)])
load_data_frame(spark, f'{TRAIN_MATCH_TABLE_PATH}/cd=2023-10-25/')\
    .join(old_prediction_df, ['date', 'content_id'])\
    .withColumn(f'{FREE_RATE_LABEL}_error', F.expr(f"abs(estimated_{FREE_RATE_LABEL}/{FREE_RATE_LABEL}-1)"))\
    .withColumn(f'{SUB_RATE_LABEL}_error', F.expr(f"abs(estimated_{SUB_RATE_LABEL}/{SUB_RATE_LABEL}-1)"))\
    .withColumn(f'{FREE_WT_LABEL}_error', F.expr(f"abs(estimated_{FREE_WT_LABEL}/{FREE_WT_LABEL}-1)"))\
    .withColumn(f'{SUB_WT_LABEL}_error', F.expr(f"abs(estimated_{SUB_WT_LABEL}/{SUB_WT_LABEL}-1)"))\
    .groupBy('tournament')\
    .agg(F.count('*'), F.avg(f"{FREE_RATE_LABEL}_error"), F.avg(f"{SUB_RATE_LABEL}_error"), F.avg(f"{FREE_WT_LABEL}_error"), F.avg(f"{SUB_WT_LABEL}_error"))\
    .show(100, False)
load_data_frame(spark, f'{TRAIN_MATCH_TABLE_PATH}/cd=2023-10-25/')\
    .join(new_prediction_df, ['date', 'content_id'])\
    .withColumn(f'{FREE_RATE_LABEL}_error', F.expr(f"abs(estimated_{FREE_RATE_LABEL}/{FREE_RATE_LABEL}-1)"))\
    .withColumn(f'{SUB_RATE_LABEL}_error', F.expr(f"abs(estimated_{SUB_RATE_LABEL}/{SUB_RATE_LABEL}-1)"))\
    .withColumn(f'{FREE_WT_LABEL}_error', F.expr(f"abs(estimated_{FREE_WT_LABEL}/{FREE_WT_LABEL}-1)"))\
    .withColumn(f'{SUB_WT_LABEL}_error', F.expr(f"abs(estimated_{SUB_WT_LABEL}/{SUB_WT_LABEL}-1)"))\
    .groupBy('tournament')\
    .agg(F.count('*'), F.avg(f"{FREE_RATE_LABEL}_error"), F.avg(f"{SUB_RATE_LABEL}_error"), F.avg(f"{FREE_WT_LABEL}_error"), F.avg(f"{SUB_WT_LABEL}_error"))\
    .show(100, False)




#
# # git clone git@github.com:hotstar/live-ads-inventory-forecasting-ml.git
# # pip install pandas==1.3.5 pyarrow==12.0.1 s3fs==2023.1.0 prophet
# cd live-ads-inventory-forecasting-ml/pipeline/code
# # df.where('lower(platform)="androidtv"').where((F.col('user_segments').contains('MMD00')) | (F.col('user_segments').contains('_MALE_')))\
# #     .select('dw_d_id', 'content_id', 'country', 'language', 'platform', 'city', 'state', 'user_segments').show(1000, False)
# # df.where('lower(platform)="androidtv"').where((F.col('user_segments').contains('FMD00')) | (F.col('user_segments').contains('_FEMALE_')))\
# #     .select('dw_d_id', 'content_id', 'country', 'language', 'platform', 'city', 'state', 'user_segments').show(1000, False)
# # df.where('lower(platform)="firetv"').where((F.col('user_segments').contains('FMD00')) | (F.col('user_segments').contains('_FEMALE_')))\
# #     .select('dw_d_id', 'content_id', 'country', 'language', 'platform', 'city', 'state', 'user_segments').show(10, False)
# # df.where('lower(platform)="androidtv" and user_segments is not null').select('dw_d_id', 'content_id', 'country', 'language', 'platform', 'city', 'state', 'user_segments').show(10, False)
# # df.where('lower(platform)="androidtv" and user_segments is not null').show()
# # df.where('lower(platform)="androidtv"').groupBy('gender', 'ageBucket').count().show()
# # df.where('lower(platform)="firetv"').groupBy('platform', 'gender', 'ageBucket').count().show()
# # raw_wt = spark.sql(f'select * from {WV_TABLE} where cd = "2023-03-22"') \
# #         .where(F.col('dw_p_id').substr(-1, 1).isin(['2', 'a', 'e', '8']))
# # spark.sql(f'select * from {WV_TABLE} where cd = "2023-03-22"').where('lower(platform)="androidtv" and user_segments is not null').show()
# #
# #
# # preroll = spark.read.parquet(f'{PREROLL_INVENTORY_PATH}cd={date}') \
# #         .select('dw_d_id', 'user_segment', 'content_id', 'device_platform').dropDuplicates(['dw_d_id']) \
# #         .select('dw_d_id', 'user_segment', 'content_id', 'device_platform', make_segment_str_wrapper('user_segment').alias('preroll_cohort')).cache()
# # preroll.where('lower(device_platform)="androidtv" and user_segment is not null').where((F.col('user_segment').contains('FMD00')) | (F.col('user_segment').contains('_FEMALE_'))).show(10, False)
# from util import *
# from path import *
# # from config import *
# #
# # df = load_data_frame(spark, "")
# # a = df.collect()
# # set1 = {}
# # set2 = {}
# # for row in a:
# #     date1 = row[0]
# #     title1 = " vs ".join(sorted(row[1].strip().lower().split(" vs ")))
# #     set1[title1] = date1
# #     date2 = row[2]
# #     title2 = " vs ".join(sorted([row[3].strip().lower().replace("netherlands", "west indies"), row[4].strip().lower().replace("netherlands", "west indies")]))
# #     set2[title2] = date2
# #
# # df2 = load_hive_table(spark, "adtech.daily_predicted_vv_report").where('cd="2023-08-11"').cache()
# # l = ["2023-09-05","2023-09-02","2023-09-03","2023-09-04","2023-08-31","2023-08-30","2023-09-06","2023-09-09","2023-09-10","2023-09-12","2023-09-14","2023-09-15","2023-09-17"]
# # for row in a:
# #     title = " vs ".join(sorted(row[1].strip().lower().split(" vs ")))
# #     if title in set2:
# #         l.append(set2[title])
# #         print(set2[title])
# #     else:
# #         print("error")
# #
# #
# # for d in l:
# #     print(df2.where(f'ds="{d}"').select('free_vv').collect()[0][0])
# #
# # print("")
# # for d in l:
# #     print(df2.where(f'ds="{d}"').select('sub_vv').collect()[0][0])
# #
# # import pyspark.sql.functions as F
# #
# # df = spark.read.parquet(f'{FINAL_ALL_PREDICTION_PATH}cd=2023-08-15/').cache()
# # for col in ['platform', 'ageBucket', 'city', 'state', 'devicePrice', 'gender', 'language']:
# #     print(col)
# #     df.where('matchId = "708501"').groupby(col).agg(F.count('reach'), F.sum('reach'), F.sum('inventory')).toPandas().to_csv(col+'.csv')
# #
# #
# # for col in ['platform', 'ageBucket', 'city', 'state', 'devicePrice', 'gender', 'language']:
# #     print(col)
# #     # df\
# #     #     .where('matchId = "708501"')\
# #     #     .groupby(col).agg(F.count('reach'), F.sum('reach'), F.sum('inventory'), F.sum('inventory')/F.count('reach'))\
# #     #     .show(20, False)
# #
# #
# # df2.groupby('platform').count().show(20)
# #
# # df = df2.toPandas()
# # target = 'ad_time'
# # group_cols = ['cd']
# # cohort_cols = ['country', 'platform', 'city', 'state', 'nccs', 'device', 'gender', 'age', 'language']
# # # calculate the inventory/reach percentage for each cohort combination
# # df[target+'_ratio'] = df[target] / df.groupby(group_cols)[target].transform('sum')  # index=cd, cols=country, platform,..., target, target_ratio
# # # convert each cohort combination to one single column
# # df.groupby('plaform').count()
# # target_value_distribution_df = df.pivot_table(index=group_cols, columns=cohort_cols, values=target+'_ratio', aggfunc='sum').fillna(0)  # index=cd, cols=cohort_candidate_combination1, cohort_candidate_combination2, ...
# # # S[n+1] = (1-alpha) * S[n] + alpha * A[n+1]
# # res_df = target_value_distribution_df.ewm(alpha=alpha, adjust=False).mean().shift(1)
# # # return the last row as the prediction results
# # res_df.iloc[-1].rename(target).reset_index()  # cols=country, platform,..., target
# #
# # import sys
# # from functools import reduce
# #
# # import pandas as pd
# # import pyspark.sql.functions as F
# # from pyspark.sql.types import *
# #
# # from util import *
# # from path import *
# #
# # inventory_distribution = {}
# # reach_distribution = {}
# #
# #
# # @F.udf(returnType=StringType())
# # def unify_nccs(cohort):
# #     if cohort is not None:
# #         for x in cohort.split('|'):
# #             if x.startswith('NCCS_'):
# #                 return x
# #     return ''
# #
# #
# # @F.udf(returnType=StringType())
# # def unify_gender(cohort):
# #     if cohort is not None:
# #         for x in cohort.split('|'):
# #             if x.startswith('FMD00') or '_FEMALE_' in x:
# #                 return 'f'
# #             if x.startswith('MMD00') or '_MALE_' in x:
# #                 return 'm'
# #     return ''
# #
# #
# # @F.udf(returnType=StringType())
# # def unify_age(cohort):
# #     map_ = {
# #         "EMAIL_FEMALE_13-17": "13-17",
# #         "EMAIL_FEMALE_18-24": "18-24",
# #         "EMAIL_FEMALE_25-34": "25-34",
# #         "EMAIL_FEMALE_35-44": "35-44",
# #         "EMAIL_FEMALE_45-54": "45-54",
# #         "EMAIL_FEMALE_55-64": "55-64",
# #         "EMAIL_FEMALE_65PLUS": "65PLUS",
# #         "EMAIL_MALE_13-17": "13-17",
# #         "EMAIL_MALE_18-24": "18-24",
# #         "EMAIL_MALE_25-34": "25-34",
# #         "EMAIL_MALE_35-44": "35-44",
# #         "EMAIL_MALE_45-54": "45-54",
# #         "EMAIL_MALE_55-64": "55-64",
# #         "EMAIL_MALE_65PLUS": "65PLUS",
# #         "FB_FEMALE_13-17": "13-17",
# #         "FB_FEMALE_18-24": "18-24",
# #         "FB_FEMALE_25-34": "25-34",
# #         "FB_FEMALE_35-44": "35-44",
# #         "FB_FEMALE_45-54": "45-54",
# #         "FB_FEMALE_55-64": "55-64",
# #         "FB_FEMALE_65PLUS": "65PLUS",
# #         "FB_MALE_13-17": "13-17",
# #         "FB_MALE_18-24": "18-24",
# #         "FB_MALE_25-34": "25-34",
# #         "FB_MALE_35-44": "35-44",
# #         "FB_MALE_45-54": "45-54",
# #         "FB_MALE_55-64": "55-64",
# #         "FB_MALE_65PLUS": "65PLUS",
# #         "PHONE_FEMALE_13-17": "13-17",
# #         "PHONE_FEMALE_18-24": "18-24",
# #         "PHONE_FEMALE_25-34": "25-34",
# #         "PHONE_FEMALE_35-44": "35-44",
# #         "PHONE_FEMALE_45-54": "45-54",
# #         "PHONE_FEMALE_55-64": "55-64",
# #         "PHONE_FEMALE_65PLUS": "65PLUS",
# #         "PHONE_MALE_13-17": "13-17",
# #         "PHONE_MALE_18-24": "18-24",
# #         "PHONE_MALE_25-34": "25-34",
# #         "PHONE_MALE_35-44": "35-44",
# #         "PHONE_MALE_45-54": "45-54",
# #         "PHONE_MALE_55-64": "55-64",
# #         "PHONE_MALE_65PLUS": "65PLUS",
# #         "FMD009V0051317HIGHSRMLDESTADS": "13-17",
# #         "FMD009V0051317SRMLDESTADS": "13-17",
# #         "FMD009V0051824HIGHSRMLDESTADS": "18-24",
# #         "FMD009V0051824SRMLDESTADS": "18-24",
# #         "FMD009V0052534HIGHSRMLDESTADS": "25-34",
# #         "FMD009V0052534SRMLDESTADS": "25-34",
# #         "FMD009V0053599HIGHSRMLDESTADS": "35-99",
# #         "FMD009V0053599SRMLDESTADS": "35-99",
# #         "MMD009V0051317HIGHSRMLDESTADS": "13-17",
# #         "MMD009V0051317SRMLDESTADS": "13-17",
# #         "MMD009V0051824HIGHSRMLDESTADS": "18-24",
# #         "MMD009V0051824SRMLDESTADS": "18-24",
# #         "MMD009V0052534HIGHSRMLDESTADS": "25-34",
# #         "MMD009V0052534SRMLDESTADS": "25-34",
# #         "MMD009V0053599HIGHSRMLDESTADS": "35-99",
# #         "MMD009V0053599SRMLDESTADS": "35-99",
# #     }
# #     if cohort is not None:
# #         for x in cohort.split('|'):
# #             if x in map_:
# #                 return map_[x]
# #     return ''
# #
# #
# # @F.udf(returnType=StringType())
# # def unify_device(cohort):
# #     if cohort is not None:
# #         dc = {'A_15031263': '15-20K', 'A_94523754': '20-25K', 'A_40990869': '25-35K', 'A_21231588': '35K+'}
# #         for x in cohort.split('|'):
# #             if x in dc:
# #                 return x
# #     return ''
# #
# #
# # @F.udf(returnType=MapType(keyType=StringType(), valueType=StringType()))
# # def cohort_enhance(cohort, ad_time, reach, cohort_col_name):
# #     global inventory_distribution, reach_distribution
# #     if cohort is None or cohort == "":
# #         res = {}
# #         for key in inventory_distribution[cohort_col_name]:
# #             cohort_inv = ad_time * inventory_distribution[cohort_col_name][key]
# #             cohort_reach = reach * reach_distribution[cohort_col_name][key]
# #             res[key] = f"{cohort_inv}#{cohort_reach}"
# #     else:
# #         res = {cohort: f"{ad_time}#{reach}"}
# #     return res
# #
# #
# # # unify regular cohort names
# # def unify_regular_cohort_names(df: DataFrame, group_cols, DATE):
# #     valid_matches = spark.read.parquet(MATCH_CMS_PATH_TEMPL % DATE) \
# #         .selectExpr('startdate as cd', 'content_id').distinct()
# #     regular_cohorts = ['gender', 'age', 'country', 'language', 'platform', 'nccs', 'device', 'city', 'state']
# #     unify_df = df\
# #         .join(valid_matches, ['cd', 'content_id'])\
# #         .withColumn('nccs', unify_nccs('cohort'))\
# #         .withColumn('device', unify_device('cohort'))\
# #         .withColumn('gender', unify_gender('cohort'))\
# #         .withColumn('age', unify_age('cohort')) \
# #         .groupby(*group_cols, *regular_cohorts) \
# #         .agg(F.sum('ad_time').alias('ad_time'), F.sum('reach').alias('reach'))\
# #         .cache()
# #     # print(unify_df.count())
# #     cohort = "gender"
# #     unify_df\
# #         .where(f"{cohort} is not null and {cohort} != ''")\
# #         .groupby('cd', 'content_id', cohort)\
# #         .agg(F.sum('ad_time').alias('ad_time'),
# #              F.sum('reach').alias('reach'))\
# #         .orderBy('cd', 'content_id')\
# #         .show(1000, False)
# #     unify_df \
# #         .where(f"{cohort} is not null and {cohort} != ''") \
# #         .groupby('cd', cohort) \
# #         .agg(F.sum('ad_time').alias('ad_time'),
# #              F.sum('reach').alias('reach')) \
# #         .orderBy('cd', 'content_id') \
# #         .show(1000, False)
# #     # all_cols = unify_df.columns
# #     # global inventory_distribution, reach_distribution
# #     # inventory_distribution = {}
# #     # reach_distribution = {}
# #     # for cohort in regular_cohorts:
# #     #     dis = unify_df.where(f"{cohort} is not null and {cohort} != ''").groupby(cohort).agg(F.sum('ad_time').alias('ad_time'), F.sum('reach').alias('reach')).collect()
# #     #     inventory_distribution[cohort] = {}
# #     #     total_inv = 0.0
# #     #     total_reach = 0.0
# #     #     inventory_distribution[cohort] = {}
# #     #     reach_distribution[cohort] = {}
# #     #     for row in dis:
# #     #         inventory_distribution[cohort][row[0]] = float(row[1])
# #     #         reach_distribution[cohort][row[0]] = float(row[2])
# #     #         total_inv += float(row[1])
# #     #         total_reach += float(row[2])
# #     #     for key in inventory_distribution[cohort]:
# #     #         inventory_distribution[cohort][key] = inventory_distribution[cohort][key] / max(total_inv, 0.00001)
# #     #         reach_distribution[cohort][key] = reach_distribution[cohort][key] / max(total_reach, 0.00001)
# #     # print(inventory_distribution['gender'])
# #     # # print(reach_distribution)
# #     # print(reach_distribution['gender'])
# #
# #
# # cd = "2023-08-15"
# # last_cd = []
# # last_cd0 = get_last_cd(INVENTORY_SAMPLING_PATH, cd, 1000)  # recent 30 days on which there are matches
# # for x in last_cd0:
# #     last_cd.append(x)
# #     # if x.startswith("2022"):
# #     #     last_cd.append(x)
# #
# # print(last_cd)
# # lst = [spark.read.parquet(f'{INVENTORY_SAMPLING_PATH}cd={i}').withColumn('cd', F.lit(i)) for i in last_cd]
# # df = reduce(lambda x, y: x.union(y), lst)
# # # unify_regular_cohort_names(regular_cohorts_df, ['cd', 'content_id'], cd)
# # group_cols = ['cd', 'content_id']
# # valid_matches = spark.read.parquet(MATCH_CMS_PATH_TEMPL % cd) \
# #         .selectExpr('startdate as cd', 'content_id').distinct()
# # regular_cohorts = ['gender', 'age', 'country', 'language', 'platform', 'nccs', 'device', 'city', 'state']
# # unify_df = df\
# #     .join(valid_matches, ['cd', 'content_id'])\
# #     .withColumn('nccs', unify_nccs('cohort'))\
# #     .withColumn('device', unify_device('cohort'))\
# #     .withColumn('gender', unify_gender('cohort'))\
# #     .withColumn('age', unify_age('cohort')) \
# #     .groupby(*group_cols, *regular_cohorts) \
# #     .agg(F.sum('ad_time').alias('ad_time'), F.sum('reach').alias('reach'))\
# #     .cache()
# # # print(unify_df.count())
# # cohort = "gender"
# # unify_df\
# #     .where(f"{cohort} is not null and {cohort} != ''")\
# #     .groupby('cd', 'content_id', cohort)\
# #     .agg(F.sum('ad_time').alias('ad_time'),
# #          F.sum('reach').alias('reach'))\
# #     .orderBy('cd', 'content_id', cohort)\
# #     .show(1000, False)
# #
# # unify_df \
# #     .where(f"{cohort} is not null and {cohort} != ''") \
# #     .groupby('cd', cohort) \
# #     .agg(F.sum('ad_time').alias('ad_time'),
# #          F.sum('reach').alias('reach')) \
# #     .orderBy('cd', cohort) \
# #     .show(1000, False)
# #
# #
# # spark.stop()
# # spark = hive_spark("etl")
# # base_cid = 1540018975
# # date = "2022-10-21"
# # wv = load_data_frame(spark, f"s3://hotstar-ads-ml-us-east-1-prod/data_exploration/data/data_backup/watched_video/cd={date}") \
# #     .where(f'content_id="{base_cid}"')\
# #     .select('dw_d_id', 'content_id', 'user_segments')\
# #     .withColumn('cohort', parse_wv_segments('user_segments'))\
# #     .withColumn('age', unify_age('cohort'))\
# #     .withColumn('gender', unify_gender('cohort'))\
# #     .cache()
# #
# # print(wv.count())
# # wv.show(10, False)
# # wv.groupby('gender').agg(F.expr('count(distinct dw_d_id) as reach')).show()
# # wv.groupby('age').agg(F.expr('count(distinct dw_d_id) as reach')).show()
# #
#
# import sys
# from functools import reduce
#
# import pandas as pd
# import pyspark.sql.functions as F
# from pyspark.sql.types import *
#
# from util import *
# from path import *
#
#
# def load_regular_cohorts_data(cd, n=30) -> DataFrame:
#     last_cd = get_last_cd(INVENTORY_SAMPLING_PATH, cd, n)  # recent 30 days on which there are matches
#     print(last_cd)
#     lst = [spark.read.parquet(f'{INVENTORY_SAMPLING_PATH}cd={i}').withColumn('cd', F.lit(i)) for i in last_cd]
#     return reduce(lambda x, y: x.union(y), lst)
#
#
# date = "2023-08-30"
# spark.stop()
# spark = hive_spark("etl")
# df = spark.createDataFrame([('1970-01-02 00:00:00', '1540024245')], ['ts', 'content_id'])
# content_ids = load_regular_cohorts_data(cd, n=4).cache()
# content_ids.select('cd', 'content_id').distinct().show(10, False)
# dates = [item[0] for item in content_ids.select('cd').distinct().collect()]
# print(dates)
# new_match_df = []
# for date in dates:
#     print(date)
#     new_match_df.append(add_labels_to_new_matches(spark, date, content_ids.where(f'cd="{date}"')))
#
#
# res = reduce(lambda x, y: x.union(y), new_match_df).cache()
# res.drop('playout_id', 'language', 'platform', 'country', 'city', 'state', 'cohort', 'ad_time', 'reach').distinct().show(100, False)
#
#
#
# save_data_frame(res.drop('playout_id', 'language', 'platform', 'country', 'city', 'state', 'cohort', 'ad_time', 'reach').distinct(), PIPELINE_BASE_PATH+"/inventory_label/icc_world_test_championship")
#
# import sys
#
# import pandas as pd
# import pyspark.sql.functions as F
# from pyspark.sql.types import *
#
# from util import *
# from path import *
# from config import *
#
#
# def make_segment_str(lst):
#     filtered = set()
#     equals = ['A_15031263', 'A_94523754', 'A_40990869', 'A_21231588']  # device price
#     prefixs = ['NCCS_', 'CITY_', 'STATE_', 'FMD00', 'MMD00', 'P_', 'R_F', 'R_M']
#     middles = ['_MALE_', '_FEMALE_']
#     for t in lst:
#         match = False
#         for s in equals:
#             if t == s:
#                 match = True
#                 break
#         if not match:
#             for s in prefixs:
#                 if t.startswith(s):
#                     match = True
#                     break
#         if not match:
#             for s in middles:
#                 if s in t:
#                     match = True
#                     break
#         if match:
#             filtered.add(t)
#     return '|'.join(sorted(filtered))
#
#
# @F.udf(returnType=StringType())
# def parse_preroll_segment(lst):
#     if lst is None:
#         return None
#     if type(lst) == str:
#         lst = lst.split(",")
#     return make_segment_str(lst)
#
#
# @F.udf(returnType=StringType())
# def parse_wv_segments(segments):
#     if segments is None:
#         return None
#     try:
#         js = json.loads(segments)
#     except:
#         return None
#     if type(js) == list:
#         lst = js
#     elif type(js) == dict:
#         lst = js.get('data', [])
#     else:
#         return None
#     return make_segment_str(lst)
#
#
# @F.udf(returnType=TimestampType())
# def parse_timestamp(date: str, ts: str):
#     return pd.Timestamp(date + ' ' + ts, tz='asia/kolkata')
#
#
# def preprocess_playout(df):
#     return df\
#         .withColumn('break_start', parse_timestamp('Start Date', 'Start Time')) \
#         .withColumn('break_end', parse_timestamp('End Date', 'End Time')) \
#         .selectExpr(
#             '`Content ID` as content_id',
#             'trim(lower(`Playout ID`)) as playout_id',
#             'trim(lower(Language)) as language',
#             'trim(lower(Tenant)) as country',
#             'explode(split(trim(lower(Platform)), "\\\\|")) as platform',
#             'break_start',
#             'break_end',
#         )\
#         .where('break_start is not null and break_end is not null') \
#         .withColumn('break_start', F.expr('cast(break_start as long)')) \
#         .withColumn('break_end', F.expr('cast(break_end as long)'))
#
#
# spark.stop()
# spark = hive_spark('etl')
# # # s = """SELECT count(distinct dw_d_id) AS reach, SUM(ad_inventory) AS preroll_inventory, content_type,substring(demo_gender, 1, 2) AS demo_gender,content_id
# # # FROM adtech.pk_inventory_metrics_daily
# # # WHERE ad_placement='Preroll' and content_id in ('1540018945','1540018948','1540019085','1540019088','1540018957','1540018960','1540019091','1540019094','1540018969','1540018972','1540018975','1540018978','1540018981','1540018984','1540018987','1540018990','1540018993','1540018996','1540018999','1540019002','1540019008','1540019011','1540019097','1540019020','1540019023','1540019026','1540019100','1540019029','1540019032','1540019035','1540019038','1540019041','1540019044','1540019047','1540019050','1540019053','1540019056','1540019059','1540019103','1540019062','1540019065','1540019068')
# # # AND content_type='SPORT_LIVE'
# # # AND cd>= date '2022-10-16'
# # # AND cd<= date '2022-11-14'
# # # GROUP BY content_type,4,content_id
# # # order by content_id"""
# # # spark.sql(s).show(1000, False)
# # s = """SELECT dw_d_id, substring(demo_gender, 1, 2) AS demo_gender
# # FROM adtech.pk_inventory_metrics_daily
# # WHERE ad_placement='Preroll' and content_id in ('1540018945','1540018948','1540019085','1540019088','1540018957','1540018960','1540019091','1540019094','1540018969','1540018972','1540018975','1540018978','1540018981','1540018984','1540018987','1540018990','1540018993','1540018996','1540018999','1540019002','1540019008','1540019011','1540019097','1540019020','1540019023','1540019026','1540019100','1540019029','1540019032','1540019035','1540019038','1540019041','1540019044','1540019047','1540019050','1540019053','1540019056','1540019059','1540019103','1540019062','1540019065','1540019068')
# # AND content_type='SPORT_LIVE'
# # AND cd>= date '2022-10-16'
# # AND cd<= date '2022-11-14'
# # """
# # save_data_frame(spark.sql(s), f"{PIPELINE_BASE_PATH}/data_tmp/gender_analysis/pk_inventory_metrics_daily/wc2022")
#
#
# # R_F1317,R_F1824,R_F2534,R_F3599
# # R_M1317,R_M1824,R_M2534,R_M3599
#
# @F.udf(returnType=StringType())
# def unify_gender(cohort):
#     if cohort is not None:
#         for x in cohort.split('|'):
#             if x.startswith('FMD00') or '_FEMALE_' in x:
#                 return 'f'
#             if x.startswith('MMD00') or '_MALE_' in x:
#                 return 'm'
#             if x.startswith('R_F'):
#                 return 'f_from_random'
#             if x.startswith('R_M'):
#                 return 'm_from_random'
#     return ''
#
#
# from functools import reduce
#
# watch_video_sampled_path = "s3://hotstar-ads-ml-us-east-1-prod/data_exploration/data/data_backup/watched_video/"
# wt = reduce(lambda x, y: x.union(y), [load_data_frame(spark, f"{watch_video_sampled_path}cd={date}").select('dw_d_id','content_id', 'user_segments')
#                                  for date in get_date_list("2022-10-16", 30)])\
#     .where("content_id in ('1540018945','1540018948','1540019085','1540019088','1540018957','1540018960','1540019091','1540019094','1540018969','1540018972','1540018975','1540018978','1540018981','1540018984','1540018987','1540018990','1540018993','1540018996','1540018999','1540019002','1540019008','1540019011','1540019097','1540019020','1540019023','1540019026','1540019100','1540019029','1540019032','1540019035','1540019038','1540019041','1540019044','1540019047','1540019050','1540019053','1540019056','1540019059','1540019103','1540019062','1540019065','1540019068')")\
#     .withColumn('cohort', parse_wv_segments('user_segments'))\
#     .withColumn('gender', unify_gender('cohort')).cache()
#
# wt.groupby('content_id', 'gender')\
#     .agg(F.countDistinct('dw_d_id'))\
#     .show(2000, False)
#
# wt.groupby('gender')\
#     .agg(F.countDistinct('dw_d_id'))\
#     .show(2000, False)
#
# save_data_frame(wt, f"{PIPELINE_BASE_PATH}/data_tmp/gender_analysis/sampled_wv/wc2022")
#
# old_df = load_data_frame(spark, "s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_v2/inventory_back_up_2023_08-23/cd=2023-06-11/").cache()
#
# df=load_data_frame(spark, "s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_v2/inventory/cd=2023-06-11/").cache()
#
# for col in ['playout_id', 'language', 'platform', 'country', 'city', 'state', 'cohort']:
#     old_df.groupby(col).count().orderBy(col).show(30, False)
#     df.groupby(col).count().orderBy(col).show(30, False)
#
# old_df.where('cohort = "A_15031263|CITY_?|NCCS_A|PHONE_BARC_MALE_15-21|PHONE_MALE_18-24|PHONE_MALE_TV_15-21|STATE_?"').show(20, False)
# df.where('cohort = "A_15031263|CITY_?|NCCS_A|PHONE_BARC_MALE_15-21|PHONE_MALE_18-24|PHONE_MALE_TV_15-21|STATE_?"').show(20, False)
#
#
# spark.sql(f'select * from {WV_TABLE} where cd = "2023-06-11"') \
#     .where(F.col('dw_p_id').substr(-1, 1).isin(['2', 'a', 'e', '8']))\
#     .groupby(F.expr('lower(language)'))\
#     .count()\
#     .show(500, False)
#
# # +----------------+---------+
# # |lower(language) |count    |
# # +----------------+---------+
# # |unknown language|856      |
# # |italian         |71       |
# # |hindi           |19515625 |
# # |serbian         |270      |
# # |malayalam       |865402   |
# # |മലയാളം          |5        |
# # |bengali         |2123700  |
# # |turkish         |2031     |
# # |french          |145      |
# # |japanese        |15381    |
# # |null            |141030696|
# # |marathi         |1075672  |
# # |english         |1925966  |
# # |hindi 100%      |2446     |
# # |odia            |7328     |
# # |na              |18       |
# # |tamil           |3335050  |
# # |kannada         |303684   |
# # |bangla          |4        |
# # |korean          |59000    |
# # |                |3219     |
# # |spanish         |408      |
# # |estonian        |45       |
# # |telugu          |2758676  |
# # +----------------+---------+
# spark.sql(f'select * from {WV_TABLE} where cd = "2023-06-11"') \
#     .where(F.col('dw_p_id').substr(-1, 1).isin(['2', 'a', 'e', '8']))\
#     .groupby(F.expr('lower(audio_language)'))\
#     .count()\
#     .show(500, False)
#
# run_date = "2023-09-11"
# cms_df = load_data_frame(spark, MATCH_CMS_PATH_TEMPL % run_date).selectExpr('content_id', 'startdate', 'lower(title)').collect()
# cid_mapping = {}
#
# for row in cms_df:
#     if row[1] not in cid_mapping:
#         cid_mapping[row[1]] = []
#     cid_mapping[row[1]].append([row[0], row[2]])
#
#
# @F.udf(returnType=StringType())
# def get_cms_content_id(date, team1, team2, raw_content_id):
#     global cid_mapping
#     if date in cid_mapping:
#         for match in cid_mapping[date]:
#             if f"{team1} vs {team2}" in match[1] or f"{team2} vs {team1}" in match[1]:
#                 return match[0]
#             if team1 in SHORT_TEAM_MAPPING and team2 in SHORT_TEAM_MAPPING:
#                 if f"{SHORT_TEAM_MAPPING[team1]} vs {SHORT_TEAM_MAPPING[team2]}" in match[1] or f"{SHORT_TEAM_MAPPING[team2]} vs {SHORT_TEAM_MAPPING[team1]}" in match[1]:
#                     return match[0]
#     return raw_content_id
#
#
# load_data_frame(spark, PREDICTION_MATCH_TABLE_PATH + f"/cd=2023-09-07")\
#     .withColumn('new_cid', get_cms_content_id('date', 'team1', 'team2', 'content_id'))\
#     .select('date', 'team1', 'team2', 'content_id', 'new_cid')\
#     .orderBy('date')\
#     .show(20, False)

#
#
# run_date = "2023-09-01"
# the_day_before_run_date = get_date_list(run_date, -2)[0]
# gt_dau_df = load_data_frame(spark, f'{DAU_TRUTH_PATH}cd={run_date}/').withColumnRenamed('ds', 'date').cache()
# gt_inv_df = load_data_frame(spark, f'{TRAIN_MATCH_TABLE_PATH}/cd={run_date}/')\
#     .where(f'date="{the_day_before_run_date}"')\
#     .selectExpr('date', 'content_id', *LABEL_COLS)\
#     .withColumn('overall_vv', F.expr('match_active_free_num+match_active_sub_num'))\
#     .withColumn('avod_wt', F.expr('match_active_free_num*watch_time_per_free_per_match'))\
#     .withColumn('svod_wt', F.expr('match_active_sub_num*watch_time_per_subscriber_per_match'))\
#     .withColumn('overall_wt', F.expr('avod_wt+svod_wt'))\
#     .withColumn('avod_reach', F.expr('total_reach*(match_active_free_num/(match_active_free_num+match_active_sub_num))'))\
#     .withColumn('svod_reach', F.expr('total_reach*(match_active_sub_num/(match_active_free_num+match_active_sub_num))'))\
#     .join(gt_dau_df, 'date')\
#     .selectExpr('date', 'content_id', 'vv as overall_dau', 'free_vv as avod_dau', 'sub_vv as svod_dau',
#                 'overall_vv', 'match_active_free_num as avod_vv', 'match_active_sub_num as svod_vv',
#                 'overall_wt', 'avod_wt', 'svod_wt', 'total_inventory',
#                 'total_reach', 'avod_reach', 'svod_reach')\
#     .cache()
# cols = gt_inv_df.columns[2:]
# for col in cols:
#     gt_inv_df = gt_inv_df.withColumn(col, F.expr(f'{col} / 1000000.0'))
#
# gt_inv_df.show(20, False)
#
# # factor = 1.0
# factor = 1.3
# predict_dau_df = load_data_frame(spark, f'{DAU_FORECAST_PATH}cd={the_day_before_run_date}/')\
#     .withColumnRenamed('ds', 'date')\
#     .withColumn('vv', F.expr(f"vv * {factor}"))\
#     .withColumn('free_vv', F.expr(f"free_vv * {factor}"))\
#     .withColumn('sub_vv', F.expr(f"vv - free_vv"))\
#     .cache()
# a = gt_dau_df.replace()
# predict_inv_df = load_data_frame(spark, f'{TOTAL_INVENTORY_PREDICTION_PATH}/cd={the_day_before_run_date}/')\
#     .where(f'date="{the_day_before_run_date}"')\
#     .withColumn('overall_vv', F.expr('estimated_reach/0.85'))\
#     .withColumn('avod_vv', F.expr('estimated_free_match_number/0.85'))\
#     .withColumn('svod_vv', F.expr('estimated_sub_match_number/0.85'))\
#     .withColumn('avod_wt', F.expr('estimated_free_match_number * estimated_watch_time_per_free_per_match'))\
#     .withColumn('svod_wt', F.expr('estimated_sub_match_number * estimated_watch_time_per_subscriber_per_match'))\
#     .withColumn('overall_wt', F.expr('avod_wt+svod_wt'))\
#     .join(predict_dau_df, 'date')\
#     .selectExpr('date', 'content_id', 'vv as overall_dau', 'free_vv as avod_dau', 'sub_vv as svod_dau',
#                 'overall_vv', 'avod_vv', 'svod_vv',
#                 'overall_wt', 'avod_wt', 'svod_wt', 'estimated_inventory as total_inventory',
#                 'estimated_reach as total_reach', 'estimated_free_match_number as avod_reach', 'estimated_sub_match_number as svod_reach')
# cols = predict_inv_df.columns[2:]
# for col in cols:
#     predict_inv_df = predict_inv_df.withColumn(col, F.expr(f'{col} / 1000000.0'))
#
# predict_inv_df.show(20, False)
#
#
# # get break list with break_start_time, break_end_tim
#
#
# import sys
#
# import pandas as pd
#
# from util import *
# from path import *
#
#
# def parse(string):
#     if string is None or string == '':
#         return False
#     lst = [x.lower() for x in json.loads(string)]
#     return lst
#
#
# def combine_inventory_and_sampling(cd):
#     model_predictions = spark.read.parquet(f'{TOTAL_INVENTORY_PREDICTION_PATH}cd={cd}/').toPandas()
#     reach_ratio = pd.read_parquet(f'{REACH_SAMPLING_PATH}cd={cd}/')
#     ad_time_ratio = pd.read_parquet(f'{AD_TIME_SAMPLING_PATH}cd={cd}/')
#     ad_time_ratio.rename(columns={'ad_time': 'inventory'}, inplace=True)
#     processed_input = pd.read_parquet(PREPROCESSED_INPUT_PATH + f'cd={cd}/')
#     # sampling match one by one
#     for i, row in model_predictions.iterrows():
#         reach = reach_ratio.copy()
#         inventory = ad_time_ratio.copy()
#         # calculate predicted inventory and reach for each cohort
#         reach.reach *= row.estimated_reach
#         inventory.inventory *= row.estimated_inventory
#         common_cols = list(set(reach.columns) & set(inventory.columns))
#         combine = inventory.merge(reach, on=common_cols, how='left')
#         # add meta data for each match
#         row.request_id = str(row.request_id)
#         row.match_id = int(row.match_id)
#         combine['request_id'] = row.request_id
#         combine['matchId'] = row.match_id
#         # We assume that matchId is unique for all requests
#         # meta_info = processed_input[(processed_input.requestId == row.request_id)&(processed_input.matchId == row.match_id)]
#         meta_info = processed_input[(processed_input.matchId == row.match_id)].iloc[0]
#         combine['tournamentId'] = meta_info['tournamentId']
#         combine['seasonId'] = meta_info['seasonId']
#         combine['adPlacement'] = 'MIDROLL'
#         # process cases when languages of this match are incomplete
#         languages = parse(meta_info.contentLanguages)
#         if languages:
#             combine.reach *= combine.reach.sum() / combine[combine.language.isin(languages)].reach.sum()
#             combine.inventory *= combine.inventory.sum() / combine[combine.language.isin(languages)].inventory.sum()
#             combine = combine[combine.language.isin(languages)].reset_index(drop=True)
#         # process case when platforms of this match are incomplete
#         platforms = parse(meta_info.platformsSupported)
#         if platforms:
#             combine.reach *= combine.reach.sum() / combine[combine.platform.isin(platforms)].reach.sum()
#             combine.inventory *= combine.inventory.sum() / combine[combine.platform.isin(platforms)].inventory.sum()
#             combine = combine[combine.platform.isin(platforms)].reset_index(drop=True)
#         combine.inventory = combine.inventory.astype(int)
#         combine.reach = combine.reach.astype(int)
#         combine.replace(
#             {'device': {'15-20K': 'A_15031263', '20-25K': 'A_94523754', '25-35K': 'A_40990869', '35K+': 'A_21231588'}},
#             inplace=True)
#         combine = combine.rename(columns={
#             'age': 'ageBucket',
#             'device': 'devicePrice',
#             'request_id': 'inventoryId',
#             'custom_cohorts': 'customCohort',
#         })
#         combine = combine[(combine.inventory >= 1)
#                           & (combine.reach >= 1)
#                           & (combine.city.map(len) != 1)].reset_index(drop=True)
#         print(combine)
#         break
#
#
# combine_inventory_and_sampling(cd="2023-09-01")
#
# import pyspark.sql.functions as F
#
#
# def combine_inventory_and_sampling(cd):
#     model_predictions = spark.read.parquet(f'{TOTAL_INVENTORY_PREDICTION_PATH}cd={cd}/')
#     reach_ratio = spark.read.parquet(f'{PREROLL_REACH_RATIO_RESULT_PATH}cd={cd}/')
#     ad_time_ratio = spark.read.parquet(f'{PREROLL_INVENTORY_RATIO_RESULT_PATH}cd={cd}/')
#     ad_time_ratio = ad_time_ratio.withColumnRenamed('ad_time', 'inventory')
#     processed_input = spark.read.parquet(PREPROCESSED_INPUT_PATH + f'cd={cd}/')
#     # sampling match one by one
#     for i, row in model_predictions.toPandas().iterrows():
#         reach = reach_ratio.select("*")
#         inventory = ad_time_ratio.select("*")
#         # calculate predicted inventory and reach for each cohort
#         reach = reach.withColumn('reach', reach['reach'] * row['estimated_reach'])
#         inventory = inventory.withColumn('inventory', inventory['inventory'] * row['estimated_preroll_inventory'])
#         common_cols = list(set(reach.columns) & set(inventory.columns))
#         combine = inventory.join(reach, on=common_cols, how='left')
#         # add meta data for each match
#         combine = combine.withColumn('request_id', F.lit(str(row['request_id'])))
#         combine = combine.withColumn('matchId', F.lit(int(row['match_id'])))
#         meta_info = processed_input.filter(processed_input['matchId'] == row['match_id']).first()
#         combine = combine.withColumn('tournamentId', F.lit(meta_info['tournamentId']))
#         combine = combine.withColumn('seasonId', F.lit(meta_info['seasonId']))
#         combine = combine.withColumn('adPlacement', F.lit('PREROLL'))
#         # process cases when languages of this match are incomplete
#         languages = parse(meta_info['contentLanguages'])
#         print(languages)
#         # combine.show()
#         if languages:
#             language_sum = combine.groupby('adPlacement').agg(F.sum('reach'), F.sum('inventory')).collect()[0]
#             filter_language_sum = combine.filter(col('language').isin(languages)).groupby('adPlacement').agg(F.sum('reach'), F.sum('inventory')).collect()[0]
#             print(language_sum)
#             combine = combine.withColumn('reach', combine['reach'] * language_sum[1] / filter_language_sum[1])
#             combine = combine.withColumn('inventory', combine['inventory'] * language_sum[2] / filter_language_sum[2])
#             combine = combine.filter(col('language').isin(languages))
#         # process case when platforms of this match are incomplete
#         platforms = parse(meta_info['platformsSupported'])
#         print(platforms)
#         if platforms:
#             platform_sum = combine.groupby('adPlacement').agg(F.sum('reach'), F.sum('inventory')).collect()[0]
#             filter_platform_sum = combine.filter(col('platform').isin(platforms)).groupby('adPlacement').agg(F.sum('reach'),
#                                                                                        F.sum('inventory')).collect()[0]
#             print(platform_sum)
#             combine = combine.withColumn('reach', combine['reach'] * platform_sum[1] / filter_platform_sum[1])
#             combine = combine.withColumn('inventory', combine['inventory'] * platform_sum[2] / filter_platform_sum[2])
#             combine = combine.filter(col('platform').isin(platforms))
#         combine = combine.withColumn('inventory', combine['inventory'].cast('integer'))
#         combine = combine.withColumn('reach', combine['reach'].cast('integer'))
#         # combine = combine.replace(
#         #    {'device': {'15-20K': 'A_15031263', '20-25K': 'A_94523754', '25-35K': 'A_40990869', '35K+': 'A_21231588'}},
#         #    subset=['device']
#         # )
#         combine = combine.withColumnRenamed('age', 'ageBucket')
#         combine = combine.withColumnRenamed('device', 'devicePrice')
#         combine = combine.withColumnRenamed('request_id', 'inventoryId')
#         combine = combine.withColumnRenamed('custom_cohorts', 'customCohort')
#         combine = combine.filter((combine['inventory'] >= 1) & (combine['reach'] >= 1) & (F.length(combine['city']) != 1))
#         print(combine.count())
#         combine.show()
#
#
# combine_inventory_and_sampling(cd="2023-09-01")
#
#
# import pyspark.sql.functions as F
# from pyspark.sql.types import *
# import pandas as pd
#
# from path import *
# from util import *
#
#
# @F.udf(returnType=ArrayType(ArrayType(IntegerType())))
# def maximum_total_duration(intervals):
#     intervals.sort(key=lambda x: x[1])  # 按结束时间升序排序
#     n = len(intervals)
#     dp = [0] * n
#     dp[0] = intervals[0][1] - intervals[0][0]
#     selected_intervals = [[intervals[0]]]  # 存储选择的二元组集合
#     for i in range(1, n):
#         max_duration = intervals[i][1] - intervals[i][0]
#         max_interval_set = [intervals[i]]
#         for j in range(i):
#             if intervals[j][1] <= intervals[i][0]:
#                 duration = dp[j] + (intervals[i][1] - intervals[i][0])
#                 if duration > max_duration:
#                     max_duration = duration
#                     max_interval_set = selected_intervals[j] + [intervals[i]]
#         dp[i] = max_duration
#         selected_intervals.append(max_interval_set)
#     # return [f"{interval[0]}#{interval[1]}" for interval in selected_intervals[n-1]]
#     return selected_intervals[n-1]
#
#
# # get break list with break_start_time, break_end_time
# def break_info_processing(playout_df, date):
#     # res_df = playout_df \
#     #     .withColumn('interval', F.array(F.col('break_start_time_int'), F.col('break_end_time_int')))\
#     #     .groupby('content_id')\
#     #     .agg(F.collect_list('interval').alias('intervals'))\
#     #     .withColumn('max_interval', maximum_total_duration('intervals'))\
#     #     .select('content_id', F.explode('max_interval').alias('interval'))\
#     #     .withColumn('break_start_time_int', F.element_at(F.col('interval'), 1))\
#     #     .withColumn('break_end_time_int', F.element_at(F.col('interval'), 2))
#     cols = ['content_id', 'break_start_time_int', 'break_end_time_int']
#     playout_df = playout_df \
#         .withColumn('rank', F.expr('row_number() over (partition by content_id order by break_start_time_int, break_end_time_int)')) \
#         .withColumn('rank_next', F.expr('rank+1'))
#     res_df = playout_df \
#         .join(playout_df.selectExpr('content_id', 'rank_next as rank', 'break_end_time_int as break_end_time_int_next'),
#               ['content_id', 'rank']) \
#         .withColumn('bias', F.expr('break_start_time_int - break_end_time_int_next')) \
#         .where('bias >= 0') \
#         .orderBy('break_start_time_int')
#     res_df = playout_df \
#         .where('rank = 1') \
#         .select(*cols) \
#         .union(res_df.select(*cols))
#     # save_data_frame(res_df, PIPELINE_BASE_PATH + f"/label/break_info/cd={date}")
#     # res_df = load_data_frame(spark, PIPELINE_BASE_PATH + f"/label/break_info/cd={date}")
#     return res_df
#
#
# # reformat playout logs
# def reformat_playout_df(playout_df):
#     return playout_df\
#         .withColumnRenamed(CONTENT_ID_COL2, 'content_id')\
#         .withColumnRenamed(START_TIME_COL2, 'start_time')\
#         .withColumnRenamed(END_TIME_COL2, 'end_time')\
#         .withColumnRenamed(PLATFORM_COL2, 'platform')\
#         .withColumnRenamed(TENANT_COL2, 'tenant')\
#         .withColumnRenamed(CONTENT_LANGUAGE_COL2, 'content_language')\
#         .withColumnRenamed(CREATIVE_ID_COL2, 'creative_id')\
#         .withColumnRenamed(BREAK_ID_COL2, 'break_id')\
#         .withColumnRenamed(PLAYOUT_ID_COL2, 'playout_id')\
#         .withColumnRenamed(CREATIVE_PATH_COL2, 'creative_path')\
#         .withColumnRenamed(CONTENT_ID_COL, 'content_id')\
#         .withColumnRenamed(START_TIME_COL, 'start_time')\
#         .withColumnRenamed(END_TIME_COL, 'end_time')\
#         .withColumnRenamed(PLATFORM_COL, 'platform')\
#         .withColumnRenamed(TENANT_COL, 'tenant')\
#         .withColumnRenamed(CONTENT_LANGUAGE_COL, 'content_language')\
#         .withColumnRenamed(CREATIVE_ID_COL, 'creative_id')\
#         .withColumnRenamed(BREAK_ID_COL, 'break_id')\
#         .withColumnRenamed(PLAYOUT_ID_COL, 'playout_id')\
#         .withColumnRenamed(CREATIVE_PATH_COL, 'creative_path') \
#         .withColumn('content_id', F.trim(F.col('content_id')))
#
#
# @F.udf(returnType=TimestampType())
# def parse_timestamp(date: str, ts: str):
#     return pd.Timestamp(date + ' ' + ts)
#
#
# # playout data processing
# def playout_data_processing(spark, date):
#     playout_df = load_data_frame(spark, f"{PLAY_OUT_LOG_INPUT_PATH}/{date}", 'csv', True)\
#         .withColumn('date', F.lit(date)) \
#         .withColumn('break_start', parse_timestamp('Start Date', 'Start Time')) \
#         .withColumn('break_end', parse_timestamp('End Date', 'End Time')) \
#         .where('break_start is not null and break_end is not null') \
#         .withColumn('break_start_time_int', F.expr('cast(break_start as long)')) \
#         .withColumn('break_end_time_int', F.expr('cast(break_end as long)'))\
#         .selectExpr('date', '`Content ID` as content_id', 'break_start_time_int', 'break_end_time_int', '`Creative Path` as creative_path') \
#         .withColumn('duration', F.expr('break_end_time_int-break_start_time_int'))\
#         .where('duration > 0 and duration < 3600 and creative_path != "aston"')
#     # save_data_frame(playout_df, PIPELINE_BASE_PATH + '/label' + PLAYOUT_LOG_PATH_SUFFIX + f"/cd={date}")
#     # playout_df = load_data_frame(spark, PIPELINE_BASE_PATH + '/label' + PLAYOUT_LOG_PATH_SUFFIX + f"/cd={date}")
#     return playout_df
#
#
# def load_break_info_from_playout_logs(spark, date):
#     playout_df = playout_data_processing(spark, date)
#     playout_df.groupby('content_id').sum('break_start_time_int', 'break_end_time_int').show()
#     # print(playout_df.count())
#     # print(playout_df.select('break_start_time_int').distinct().count())
#     break_info_df = break_info_processing(playout_df, date)
#     break_info_df.groupby('content_id').sum('break_start_time_int', 'break_end_time_int').show()
#     return break_info_df
#
#
# def load_wv_data(spark, date):
#     data_source = "watched_video"
#     timestamp_col = "ts_occurred_ms"
#     if not check_s3_path_exist(PIPELINE_BASE_PATH + f"/label/{data_source}/cd={date}"):
#         watch_video_df = spark.sql(f'select * from {WV_TABLE} where cd = "{date}"') \
#             .withColumn('timestamp', F.expr('coalesce(cast(from_unixtime(CAST(ts_occurred_ms/1000 as BIGINT)) as timestamp), timestamp) as timestamp'))\
#             .select("timestamp", 'received_at', 'watch_time', 'content_id', 'dw_p_id', 'dw_d_id') \
#             .withColumn('wv_end_timestamp', F.substring(F.col('timestamp'), 1, 19)) \
#             .withColumn('wv_end_timestamp', F.expr('if(wv_end_timestamp <= received_at, wv_end_timestamp, received_at)')) \
#             .withColumn('watch_time', F.expr('cast(watch_time as int)')) \
#             .withColumn('wv_start_timestamp', F.from_unixtime(F.unix_timestamp(F.col('wv_end_timestamp'), 'yyyy-MM-dd HH:mm:ss') - F.col('watch_time'))) \
#             .withColumn('wv_start_timestamp', F.from_utc_timestamp(F.col('wv_start_timestamp'), "IST")) \
#             .withColumn('wv_end_timestamp', F.from_utc_timestamp(F.col('wv_end_timestamp'), "IST")) \
#             .withColumn('wv_start_time_int',
#                         F.expr('cast(unix_timestamp(wv_start_timestamp, "yyyy-MM-dd HH:mm:ss") as long)')) \
#             .withColumn('wv_end_time_int',
#                         F.expr('cast(unix_timestamp(wv_end_timestamp, "yyyy-MM-dd HH:mm:ss") as long)')) \
#             .drop('received_at', 'timestamp', 'wv_start_timestamp', 'wv_end_timestamp') \
#             .cache()
#         save_data_frame(watch_video_df, PIPELINE_BASE_PATH + f"/label/{data_source}/cd={date}")
#     watch_video_df = load_data_frame(spark, PIPELINE_BASE_PATH + f"/label/{data_source}/cd={date}") \
#         .cache()
#     return watch_video_df
#
#
# date = "2023-08-30"
# break_info_df1 = load_break_info_from_playout_logs(spark, date).cache()
# watch_video_df = load_wv_data(spark, date)
# # calculate inventory and reach, need to extract the common intervals for each users
# total_inventory_df = watch_video_df\
#     .join(F.broadcast(break_info_df1), ['content_id'])\
#     .withColumn('valid_duration', F.expr('least(wv_end_time_int, break_end_time_int) - greatest(wv_start_time_int, break_start_time_int)'))\
#     .where('valid_duration > 0')\
#     .groupBy('content_id')\
#     .agg(F.sum('valid_duration').alias('total_duration'),
#          F.countDistinct("dw_d_id").alias('total_reach'))\
#     .withColumn('total_inventory', F.expr(f'cast((total_duration / 10) as bigint)')) \
#     .withColumn('total_reach', F.expr(f'cast(total_reach as bigint)'))
# total_inventory_df.show(20, False)
#
#
# the_day_before_run_date = get_date_list(run_date, -2)[0]
# last_update_date = get_last_cd(TOTAL_INVENTORY_PREDICTION_PATH, end=run_date)
# print(the_day_before_run_date)
# print(last_update_date)
# gt_dau_df = load_data_frame(spark, f'{DAU_TRUTH_PATH}cd={run_date}/')\
#     .withColumnRenamed('ds', 'date')\
#     .where(f'date="{the_day_before_run_date}"')\
#     .cache()
# gt_dau_df.show()
# gt_inv_df = load_data_frame(spark, f'{TRAIN_MATCH_TABLE_PATH}/cd={run_date}/') \
#     .where(f'date="{the_day_before_run_date}"') \
#     .selectExpr('date', 'teams', *LABEL_COLS) \
#     .withColumn('overall_vv', F.expr('match_active_free_num+match_active_sub_num')) \
#     .withColumn('avod_wt', F.expr('match_active_free_num*watch_time_per_free_per_match')) \
#     .withColumn('svod_wt', F.expr('match_active_sub_num*watch_time_per_subscriber_per_match')) \
#     .withColumn('overall_wt', F.expr('avod_wt+svod_wt')) \
#     .withColumn('avod_reach',
#                 F.expr('total_reach*(match_active_free_num/(match_active_free_num+match_active_sub_num))')) \
#     .withColumn('svod_reach',
#                 F.expr('total_reach*(match_active_sub_num/(match_active_free_num+match_active_sub_num))')) \
#     .join(gt_dau_df, 'date') \
#     .selectExpr('date', 'teams', 'vv as overall_dau', 'free_vv as avod_dau', 'sub_vv as svod_dau',
#                 'overall_vv', 'match_active_free_num as avod_vv', 'match_active_sub_num as svod_vv',
#                 'overall_wt', 'avod_wt', 'svod_wt', 'total_inventory',
#                 'total_reach', 'avod_reach', 'svod_reach') \
#     .cache()
# cols = gt_inv_df.columns[2:]
# for col in cols:
#     gt_inv_df = gt_inv_df.withColumn(col, F.expr(f'round({col} / 1000000.0, 1)'))
# publish_to_slack(topic=SLACK_NOTIFICATION_TOPIC, title="ground truth of matches", output_df=gt_inv_df, region=REGION)
#
#
# load_data_frame(spark, f'{WATCH_AGGREGATED_INPUT_PATH}/cd=2023-09-03', fmt="orc").where('content_id="1540024251"').groupBy('content_id') \
#         .agg(F.countDistinct('dw_p_id').alias('match_active_sub_num'),
#              F.sum('watch_time').alias('total_watch_time')).show()
#
# load_data_frame(spark, f'{WATCH_AGGREGATED_INPUT_PATH}/cd=2023-09-02', fmt="orc")\
#     .where('content_id="1540024251"')\
#     .select('content_id', 'dw_p_id', 'watch_time')\
#     .groupBy('content_id') \
#     .agg(F.countDistinct('dw_p_id').alias('match_active_sub_num'), F.sum('watch_time').alias('total_watch_time')).show()
#
# # +----------+--------------------+--------------------+
# # |content_id|match_active_sub_num|    total_watch_time|
# # +----------+--------------------+--------------------+
# # |1540024251|            47268737|3.0633873293017254E9|
# # +----------+--------------------+--------------------+
#
# load_data_frame(spark, f'{WATCH_AGGREGATED_INPUT_PATH}/cd=2023-09-02', fmt="orc")\
#     .where('content_id="1540024251"')\
#     .select('content_id', 'dw_p_id', 'watch_time').union(load_data_frame(spark, f'{WATCH_AGGREGATED_INPUT_PATH}/cd=2023-09-03', fmt="orc")
#     .where('content_id="1540024251"')
#     .select('content_id', 'dw_p_id', 'watch_time'))\
#     .groupBy('content_id') \
#     .agg(F.countDistinct('dw_p_id').alias('match_active_sub_num'), F.sum('watch_time').alias('total_watch_time')).show()
#
# # +----------+--------------------+-----------------+
# # |content_id|match_active_sub_num| total_watch_time|
# # +----------+--------------------+-----------------+
# # |1540024251|            47556965|3.0954943893241E9|
# # +----------+--------------------+-----------------+
#
# load_data_frame(spark, f"{PLAY_OUT_LOG_INPUT_PATH}/2023-09-02", 'csv', True)\
#         .withColumn('break_start', parse_timestamp('Start Date', 'Start Time')) \
#         .withColumn('break_end', parse_timestamp('End Date', 'End Time')) \
#         .where('break_start is not null and break_end is not null') \
#         .withColumn('break_start_time_int', F.expr('cast(break_start as long)')) \
#         .withColumn('break_end_time_int', F.expr('cast(break_end as long)'))\
#         .selectExpr('`Content ID` as content_id', 'break_start_time_int', 'break_end_time_int', '`Creative Path` as creative_path') \
#         .withColumn('duration', F.expr('break_end_time_int-break_start_time_int'))\
#         .where('duration > 0 and duration < 3600 and creative_path != "aston" and content_id="1540024251"').count()
#
# spark.sql(f'select * from {WV_TABLE} where cd = "2023-09-03"').where('content_id="1540024251"').count()

# factor = 1.3
# base_date = "2023-08-21"
# load_data_frame(spark, f'{DAU_FORECAST_PATH}cd={base_date}/') \
#     .withColumnRenamed('ds', 'date') \
#     .withColumn('vv', F.expr(f"vv * {factor}")) \
#     .withColumn('free_vv', F.expr(f"free_vv * {factor}")) \
#     .withColumn('sub_vv', F.expr(f"vv - free_vv")) \
#     .where('date >= "2023-08-30"')\
#     .orderBy('date')\
#     .show()
#
#
# import pyspark.sql.functions as F
# from pyspark.sql.types import *
# import pandas as pd
# from functools import reduce
#
# from path import *
# from util import *
#
#
# # calculate free/sub reach and avg_wt
# def calculate_reach_and_wt_from_wv_table(spark, dates):
#     match_sub_df = reduce(lambda x, y: x.union(y), [load_data_frame(spark, f'{WATCH_AGGREGATED_INPUT_PATH}/cd={date}', fmt="orc") \
#         .where(f'upper(subscription_status) in ("ACTIVE", "CANCELLED", "GRACEPERIOD")') \
#         .withColumn('content_id', F.expr('if(content_id="1540025325", "1540024269", content_id)')) \
#         .groupBy('dw_p_id', 'content_id') \
#         .agg(F.sum('watch_time').alias('watch_time')) for date in dates]) \
#         .groupBy('content_id') \
#         .agg(F.countDistinct('dw_p_id').alias('match_active_sub_num'),
#              F.sum('watch_time').alias('total_watch_time')) \
#         .withColumn('watch_time_per_subscriber_per_match', F.expr('total_watch_time/match_active_sub_num')) \
#         .select('content_id', 'match_active_sub_num', 'watch_time_per_subscriber_per_match') \
#         .cache()
#     match_free_df = reduce(lambda x, y: x.union(y), [load_data_frame(spark, f'{WATCH_AGGREGATED_INPUT_PATH}/cd={date}', fmt="orc") \
#         .where(f'upper(subscription_status) not in ("ACTIVE", "CANCELLED", "GRACEPERIOD")') \
#         .withColumn('content_id', F.expr('if(content_id="1540025325", "1540024269", content_id)')) \
#         .groupBy('dw_p_id', 'content_id') \
#         .agg(F.sum('watch_time').alias('watch_time')) for date in dates]) \
#         .groupBy('content_id') \
#         .agg(F.countDistinct('dw_p_id').alias('match_active_free_num'),
#              F.sum('watch_time').alias('total_free_watch_time')) \
#         .withColumn('watch_time_per_free_per_match', F.expr('total_free_watch_time/match_active_free_num')) \
#         .select('content_id', 'match_active_free_num', 'watch_time_per_free_per_match') \
#         .cache()
#     return match_sub_df, match_free_df
#
#
# # get break list with break_start_time, break_end_time
# def break_info_processing(playout_df, date):
#     cols = ['content_id', 'break_start_time_int', 'break_end_time_int']
#     playout_df = playout_df \
#         .withColumn('rank', F.expr('row_number() over (partition by content_id order by break_start_time_int, break_end_time_int)')) \
#         .withColumn('rank_next', F.expr('rank+1'))
#     res_df = playout_df \
#         .join(playout_df.selectExpr('content_id', 'rank_next as rank', 'break_end_time_int as break_end_time_int_next'),
#               ['content_id', 'rank']) \
#         .withColumn('bias', F.expr('break_start_time_int - break_end_time_int_next')) \
#         .where('bias >= 0') \
#         .orderBy('break_start_time_int')
#     res_df = playout_df \
#         .where('rank = 1') \
#         .select(*cols) \
#         .union(res_df.select(*cols))
#     # save_data_frame(res_df, PIPELINE_BASE_PATH + f"/label/break_info/cd={date}")
#     # res_df = load_data_frame(spark, PIPELINE_BASE_PATH + f"/label/break_info/cd={date}")
#     return res_df
#
#
# # reformat playout logs
# def reformat_playout_df(playout_df):
#     return playout_df\
#         .withColumnRenamed(CONTENT_ID_COL2, 'content_id')\
#         .withColumnRenamed(START_TIME_COL2, 'start_time')\
#         .withColumnRenamed(END_TIME_COL2, 'end_time')\
#         .withColumnRenamed(PLATFORM_COL2, 'platform')\
#         .withColumnRenamed(TENANT_COL2, 'tenant')\
#         .withColumnRenamed(CONTENT_LANGUAGE_COL2, 'content_language')\
#         .withColumnRenamed(CREATIVE_ID_COL2, 'creative_id')\
#         .withColumnRenamed(BREAK_ID_COL2, 'break_id')\
#         .withColumnRenamed(PLAYOUT_ID_COL2, 'playout_id')\
#         .withColumnRenamed(CREATIVE_PATH_COL2, 'creative_path')\
#         .withColumnRenamed(CONTENT_ID_COL, 'content_id')\
#         .withColumnRenamed(START_TIME_COL, 'start_time')\
#         .withColumnRenamed(END_TIME_COL, 'end_time')\
#         .withColumnRenamed(PLATFORM_COL, 'platform')\
#         .withColumnRenamed(TENANT_COL, 'tenant')\
#         .withColumnRenamed(CONTENT_LANGUAGE_COL, 'content_language')\
#         .withColumnRenamed(CREATIVE_ID_COL, 'creative_id')\
#         .withColumnRenamed(BREAK_ID_COL, 'break_id')\
#         .withColumnRenamed(PLAYOUT_ID_COL, 'playout_id')\
#         .withColumnRenamed(CREATIVE_PATH_COL, 'creative_path') \
#         .withColumn('content_id', F.trim(F.col('content_id')))
#
#
# @F.udf(returnType=TimestampType())
# def parse_timestamp(date: str, ts: str):
#     try:
#         return pd.Timestamp(date + ' ' + ts)
#     except:
#         return None
#
#
# # playout data processing
# def playout_data_processing(spark, date):
#     bucket_name = "hotstar-ads-data-external-us-east-1-prod"
#     folder_path = f'run_log/blaze/prod/test/{date}/'
#     file_list = get_s3_paths(bucket_name, folder_path)
#     playout_df = load_multiple_csv_file(spark, file_list)\
#         .withColumn('date', F.lit(date)) \
#         .withColumn('break_start', parse_timestamp('Start Date', 'Start Time')) \
#         .withColumn('break_end', parse_timestamp('End Date', 'End Time')) \
#         .where('break_start is not null and break_end is not null') \
#         .withColumn('break_start_time_int', F.expr('cast(break_start as long)')) \
#         .withColumn('break_end_time_int', F.expr('cast(break_end as long)'))\
#         .selectExpr('date', '`Content ID` as content_id', 'break_start_time_int', 'break_end_time_int', '`Creative Path` as creative_path') \
#         .withColumn('duration', F.expr('break_end_time_int-break_start_time_int')) \
#         .withColumn('content_id', F.expr('if(content_id="1540025325", "1540024269", content_id)'))\
#         .where('duration > 0 and duration < 3600 and creative_path != "aston"')
#     print(playout_df.count())
#     return playout_df
#
#
# def load_break_info_from_playout_logs(spark, dates):
#     playout_df = playout_data_processing(spark, dates)
#     break_info_df = break_info_processing(playout_df, dates)
#     return break_info_df
#
#
# def load_wv_data(spark, date):
#     data_source = "watched_video"
#     timestamp_col = "ts_occurred_ms"
#     if not check_s3_path_exist(PIPELINE_BASE_PATH + f"/label/{data_source}/cd={date}"):
#         watch_video_df = spark.sql(f'select * from {WV_TABLE} where cd = "{date}"') \
#             .withColumn('timestamp', F.expr('coalesce(cast(from_unixtime(CAST(ts_occurred_ms/1000 as BIGINT)) as timestamp), timestamp) as timestamp'))\
#             .select("timestamp", 'received_at', 'watch_time', 'content_id', 'dw_p_id', 'dw_d_id') \
#             .withColumn('content_id', F.expr('if(content_id="1540025325", "1540024269", content_id)')) \
#             .withColumn('wv_end_timestamp', F.substring(F.col('timestamp'), 1, 19)) \
#             .withColumn('wv_end_timestamp', F.expr('if(wv_end_timestamp <= received_at, wv_end_timestamp, received_at)')) \
#             .withColumn('watch_time', F.expr('cast(watch_time as int)')) \
#             .withColumn('wv_start_timestamp', F.from_unixtime(F.unix_timestamp(F.col('wv_end_timestamp'), 'yyyy-MM-dd HH:mm:ss') - F.col('watch_time'))) \
#             .withColumn('wv_start_timestamp', F.from_utc_timestamp(F.col('wv_start_timestamp'), "IST")) \
#             .withColumn('wv_end_timestamp', F.from_utc_timestamp(F.col('wv_end_timestamp'), "IST")) \
#             .withColumn('wv_start_time_int',
#                         F.expr('cast(unix_timestamp(wv_start_timestamp, "yyyy-MM-dd HH:mm:ss") as long)')) \
#             .withColumn('wv_end_time_int',
#                         F.expr('cast(unix_timestamp(wv_end_timestamp, "yyyy-MM-dd HH:mm:ss") as long)')) \
#             .drop('received_at', 'timestamp', 'wv_start_timestamp', 'wv_end_timestamp') \
#             .cache()
#         save_data_frame(watch_video_df, PIPELINE_BASE_PATH + f"/label/{data_source}/cd={date}")
#     watch_video_df = load_data_frame(spark, PIPELINE_BASE_PATH + f"/label/{data_source}/cd={date}") \
#         .cache()
#     return watch_video_df
#
#
# # calculate midroll inventory and reach ground truth
# def calculate_midroll_inventory_and_reach_gt(spark, dates):
#     break_info_df = reduce(lambda x, y: x.union(y), [load_break_info_from_playout_logs(spark, date) for date in dates])
#     watch_video_df = reduce(lambda x, y: x.union(y), [load_wv_data(spark, date) for date in dates])
#     # calculate inventory and reach, need to extract the common intervals for each users
#     total_inventory_df = watch_video_df\
#         .join(F.broadcast(break_info_df), ['content_id'])\
#         .withColumn('valid_duration', F.expr('least(wv_end_time_int, break_end_time_int) - greatest(wv_start_time_int, break_start_time_int)'))\
#         .where('valid_duration > 0')\
#         .groupBy('content_id')\
#         .agg(F.sum('valid_duration').alias('total_duration'),
#              F.countDistinct("dw_d_id").alias('total_reach'))\
#         .withColumn('total_inventory', F.expr(f'cast((total_duration / 10) as bigint)')) \
#         .withColumn('total_reach', F.expr(f'cast(total_reach as bigint)'))\
#         .cache()
#     return total_inventory_df
#
#
# strip_udf = F.udf(lambda x: x.strip(), StringType())
#
# spark.stop()
# spark = hive_spark("dataset_update")
# match_sub_df, match_free_df = calculate_reach_and_wt_from_wv_table(spark, dates=["2023-09-10", "2023-09-11"])
# df = calculate_midroll_inventory_and_reach_gt(spark, dates=["2023-09-10", "2023-09-11"]).where('content_id="1540024269"').cache()
# df.\
#     join(match_sub_df, 'content_id')\
#     .join(match_free_df, 'content_id')\
#     .withColumn('overall_vv', F.expr('match_active_free_num+match_active_sub_num')) \
#     .withColumn('avod_wt', F.expr('match_active_free_num*watch_time_per_free_per_match')) \
#     .withColumn('svod_wt', F.expr('match_active_sub_num*watch_time_per_subscriber_per_match')) \
#     .withColumn('overall_wt', F.expr('avod_wt+svod_wt')) \
#     .withColumn('avod_reach',
#                 F.expr('total_reach*(match_active_free_num/(match_active_free_num+match_active_sub_num))')) \
#     .withColumn('svod_reach',
#                 F.expr('total_reach*(match_active_sub_num/(match_active_free_num+match_active_sub_num))')) \
#     .selectExpr('content_id',
#                     'overall_vv', 'match_active_free_num as avod_vv', 'match_active_sub_num as svod_vv', 'match_active_free_num/match_active_sub_num as vv_rate',
#                     'overall_wt', 'avod_wt', 'svod_wt', 'total_inventory',
#                     'total_reach', 'avod_reach', 'svod_reach')\
#     .show(10, False)
#
#
# import pyspark.sql.functions as F
#
# spark.stop()
# spark = hive_spark("dau_update")
# base = spark.sql(f'select cd as ds, dw_p_id, subscription_status from {DAU_TABLE} where cd > "2023-09-09" and cd < "2023-09-12"')\
#     .withColumn('tag', F.lit('date')).cache()
#
# base.groupby('tag').agg(F.countDistinct('dw_p_id').alias('vv')).show()
# base.where('lower(subscription_status) in ("active", "cancelled", "graceperiod")') \
#         .groupby('tag').agg(F.countDistinct('dw_p_id').alias('sub_vv')).show()

# import pyspark.sql.functions as F
# from pyspark.sql.types import *
# import pandas as pd
# from functools import reduce
#
# from path import *
# from util import *
#
#
# cd = "2023-09-14"
# all = []
# all_female = []
# all_random_female = []
# matches = spark.read.parquet(MATCH_CMS_PATH_TEMPL % cd).where(f'sportsseasonname = "Asia Cup 2023" and startdate < "{cd}"').toPandas()
# for dt in matches.startdate.drop_duplicates():
#     print(dt)
#     content_ids = matches[matches.startdate == dt].content_id.tolist()
#     preroll = load_data_frame(spark, PREROLL_INVENTORY_PATH + f'cd={dt}/')\
#         .where(F.col('content_id').isin(content_ids) & F.expr("lower(ad_placement) = 'preroll'"))\
#         .withColumn('gender', F.expr('lower(split(demo_gender, ",")[0])'))\
#         .withColumn('source', F.expr('lower(split(demo_source, ",")[0])'))\
#         .withColumn('date', F.lit(dt))\
#         .select('date', 'content_id', 'gender', 'source', 'dw_d_id')\
#         .cache()
#     all.append(preroll.groupby('date').agg(F.expr('count(distinct dw_d_id) as reach')))
#     all_female.append(preroll.where('gender = "female"').groupby('date').agg(F.expr('count(distinct dw_d_id) as female_reach')))
#     all_random_female.append(preroll.where('gender="female" and source="random"').groupby('date').agg(F.expr('count(distinct dw_d_id) as random_female_reach')))
#
# reduce(lambda x, y: x.union(y), all)\
#     .join(reduce(lambda x, y: x.union(y), all_female), 'date')\
#     .join(reduce(lambda x, y: x.union(y), all_random_female), 'date')\
#     .orderBy('date')\
#     .show(200, False)
#


# import pandas as pd
# import sys
# from functools import reduce
# from pyspark.sql.window import Window
# import pyspark.sql.functions as F
# from pyspark.sql.types import *
#
# from util import *
# from path import *
# import time
#
# inventory_distribution = {}
# reach_distribution = {}
# cohort_cols = ['gender', 'age_bucket', 'city', 'language', 'state', 'location_cluster', 'pincode', 'interest', 'device_brand', 'device_model',
#     'primary_sim', 'data_sim', 'platform', 'os', 'app_version', 'subscription_type', 'content_type', 'content_genre']
#
#
# # df = load_data_frame(spark, f'{AD_TIME_SAMPLING_PATH}cd=2023-09-14').cache()
# # df.groupby('city').sum('ad_time').where('city="bangalore"').orderBy('city').show(100,False)
#
# DATE = "2023-09-18"
# df = load_data_frame(spark, PREROLL_SAMPLING_ROOT_PATH + "cohort_tmp/" + "all/" + f"/cd={DATE}").cache()
# group_cols = ['cd']
# target='ad_time'
# lambda_=0.8
# df2 = df.groupby(group_cols).agg(F.sum(target).alias('gsum'))
# df3 = df.join(df2, group_cols).withColumn(target, F.col(target)/F.col('gsum'))
# group_cols = [F.col(x).desc() for x in group_cols]
# print(group_cols)
# print(time.ctime())
# df4 = df3.withColumn(target, F.col(target) * F.lit(lambda_) * F.pow(
#         F.lit(1-lambda_),
#         F.row_number().over(Window.partitionBy(*cohort_cols).orderBy(*group_cols))-F.lit(1)
#     )
# )
# print(df4.groupby(cohort_cols).agg(F.sum(target).alias(target)).count())
# print(time.ctime())
#
# src_col='watch_time'
# dst_col='ad_time'
# ch = pd.read_parquet(f'{CUSTOM_COHORT_PATH}cd={DATE}/')
# ch = ch[~(ch.is_cricket==False)]
# ch.segments.fillna('', inplace=True)
# ch2 = (ch.groupby('segments')[src_col].sum().rename(dst_col).rename_axis('custom_cohorts') / ch[src_col].sum()).reset_index()
# df2 = df4.crossJoin(F.broadcast(spark.createDataFrame(ch2).withColumnRenamed(dst_col, dst_col+"_c")))\
#     .withColumn(dst_col, F.expr(f'{dst_col} * {dst_col}_c'))\
#     .drop(dst_col+"_c")


# run_date = "2023-09-20"
# true_vv_path = f'{DAU_TRUTH_PATH}cd={run_date}/'
# if not s3.isfile(true_vv_path + '_SUCCESS'):
#     truth(run_date, true_vv_path)
#
# forecast(run_date, true_vv_path)
# combine(run_date)
# spark.read.parquet(f'{DAU_FORECAST_PATH}cd={run_date}/')\
#         .where(f'ds >= "2023-10-05"').orderBy('ds').show(30)
#
# update_dashboards()
#
#
#
# import sys
#
# import pandas as pd
# from prophet import Prophet
# import pyspark.sql.functions as F
#
# from util import *
# from path import *
#
# gt_inv_df = load_data_frame(spark, f'{TRAIN_MATCH_TABLE_PATH}/cd=2023-09-16/').cache()
# cols = gt_inv_df.columns
#
# million = 1000000
# simple_df = load_data_frame(spark, f'{TRAIN_MATCH_TABLE_PATH}/cd=2023-09-11/') \
#     .where(f'date ="2023-09-02"')\
#     .withColumn('date', F.lit('2023-09-10'))\
#     .withColumn('content_id', F.lit('1540024269'))\
#     .withColumn('total_reach', F.lit(int(66.9*million)))\
#     .withColumn('total_inventory', F.lit(7796*million))\
#     .withColumn('total_frees_number', F.lit(34786899.5))\
#     .withColumn('match_active_free_num', F.lit(56.0*million))\
#     .withColumn('total_subscribers_number', F.lit(12591464.11))\
#     .withColumn('match_active_sub_num', F.lit(13.5*million))\
#     .withColumn('preroll_sub_sessions', F.lit(-1.0))\
#     .withColumn('preroll_free_sessions', F.lit(-1.0))\
#     .withColumn('preroll_sub_inventory', F.lit(-1.0))\
#     .withColumn('preroll_free_inventory', F.lit(-1.0))\
#     .withColumn('watch_time_per_free_per_match', F.lit(4859/56))\
#     .withColumn('watch_time_per_subscriber_per_match', F.lit(1899/13.5))\
#     .withColumn('frees_watching_match_rate', F.expr('match_active_free_num/total_frees_number')) \
#     .withColumn('subscribers_watching_match_rate', F.expr('match_active_sub_num/total_subscribers_number'))\
#     .select(cols)\
#     .cache()
#
# save_data_frame(gt_inv_df.union(simple_df), f'{TRAIN_MATCH_TABLE_PATH}/cd=2023-09-17/')
#
# gt_inv_df.union(simple_df).orderBy('date').where('date >= "2023-08-30"').show()
# simple_df.show(20, False)

# cols = ['date', 'tournament', 'content_id', 'vod_type',
#         'match_stage', 'tournament_name', 'match_type',
#         'if_contain_india_team', 'if_holiday', 'match_time',
#         'if_weekend', 'tournament_type', 'teams', 'continents',
#         'teams_tier', 'free_timer', 'frees_watching_match_rate',
#         'watch_time_per_free_per_match', 'subscribers_watching_match_rate',
#         'watch_time_per_subscriber_per_match', 'reach_rate', 'total_reach',
#         'total_inventory', 'total_frees_number', 'match_active_free_num',
#         'total_subscribers_number', 'match_active_sub_num', 'preroll_sub_sessions',
#         'preroll_free_sessions', 'preroll_sub_inventory', 'preroll_free_inventory']


from util import *
from path import *

last_update_date = "2023-09-18"
RETENTION_RATE = 0.85

predict_dau_df = load_data_frame(spark, f'{DAU_FORECAST_PATH}cd={last_update_date}/') \
    .withColumnRenamed('ds', 'date') \
    .cache()
predict_inv_df = load_data_frame(spark, f'{TOTAL_INVENTORY_PREDICTION_PATH}/cd={last_update_date}/') \
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

predict_inv_df.orderBy('date').show(100, False)

run_date = "2023-09-18"
train = load_data_frame(spark, f'{TRAIN_MATCH_TABLE_PATH}/cd={run_date}/')\
    .withColumn('team1', F.element_at(F.col('teams'), 1))\
    .withColumn('team2', F.element_at(F.col('teams'), 2)).cache()
predict = load_data_frame(spark, f'{TOTAL_INVENTORY_PREDICTION_PATH}/cd={run_date}/')\
    .withColumn('teams', F.split(F.col('teams'), ' vs '))\
    .withColumn('team1', F.element_at(F.col('teams'), 1))\
    .withColumn('team2', F.element_at(F.col('teams'), 2)).cache()

l1 = []
l2 = []
for t in train.select('team1').collect():
    l1.append(t[0])

for t in train.select('team2').collect():
    l1.append(t[0])

for t in predict.select('team1').collect():
    l2.append(t[0])

for t in predict.select('team2').collect():
    l2.append(t[0])

for t in (sorted(list(set(l1+l2)))):
    print(t)

