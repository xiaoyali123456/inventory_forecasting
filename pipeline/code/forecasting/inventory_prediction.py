from path import *
from util import *
from config import *


# load prediction dataset
def load_prediction_dataset(run_date):
    prediction_feature_df = load_data_frame(spark, prediction_feature_path + f"/cd={run_date}")\
        .selectExpr('requestId as request_id', 'matchId as match_id', 'content_id', 'date', 'tournament',
                    'total_frees_number', 'total_subscribers_number')\
        .cache()
    return prediction_feature_df


# load dnn prediction results
def load_rate_and_wt_predictions_by_dnn(df, run_date):
    label_path = f"{pipeline_base_path}/dnn_predictions/cd={run_date}"
    common_cols = ['content_id']
    # load parameters predicted by dnn models
    return df \
        .join(load_data_frame(spark, f"{label_path}/label={free_rate_label}"), common_cols) \
        .join(load_data_frame(spark, f"{label_path}/label={free_wt_label}"), common_cols) \
        .join(load_data_frame(spark, f"{label_path}/label={sub_rate_label}"), common_cols) \
        .join(load_data_frame(spark, f"{label_path}/label={sub_wt_label}"), common_cols)


# for forecasting inventory at match level
def main(run_date):
    prediction_feature_df = load_prediction_dataset(run_date)
    partition_col = "request_id"
    # load parameters predicted by dnn models
    prediction_feature_df = load_rate_and_wt_predictions_by_dnn(prediction_feature_df, run_date)
    total_match_duration_in_minutes, number_of_ad_breaks, average_length_of_a_break_in_seconds = match_configuration
    res_df = prediction_feature_df \
        .withColumn('estimated_avg_concurrency', F.expr('(total_frees_number * estimated_frees_watching_match_rate * estimated_watch_time_per_free_per_match '
        f'+ total_subscribers_number * estimated_subscribers_watching_match_rate * estimated_watch_time_per_subscriber_per_match)/{total_match_duration_in_minutes}')) \
        .withColumn('estimated_inventory', F.expr(f'estimated_avg_concurrency * {retention_rate} * ({number_of_ad_breaks * average_length_of_a_break_in_seconds} / 10.0)')) \
        .withColumn('estimated_reach', F.expr(f"(total_frees_number * estimated_frees_watching_match_rate / {free_pid_did_rate}) + (total_subscribers_number * estimated_subscribers_watching_match_rate / {sub_pid_did_rate})")) \
        .withColumn('estimated_inventory', F.expr('cast(estimated_inventory as bigint)')) \
        .withColumn('estimated_reach', F.expr('cast(estimated_reach as bigint)')) \
        .cache()
    res_df.show(1000, False)
    save_data_frame(res_df, pipeline_base_path + f"/inventory_prediction/future_tournaments/cd={run_date}/", partition_col=partition_col)


if __name__ == '__main__':
    run_date = sys.argv[1]
    if check_s3_path_exist(f"{prediction_feature_path}/cd={run_date}/"):
        main(run_date)
        slack_notification(topic=slack_notification_topic, region=region,
                           message=f"inventory forecasting on {run_date} is done.")


