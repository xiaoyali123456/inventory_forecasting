from path import *
from util import *
from config import *


def load_dataset(DATE):
    # load features of prediction samples
    prediction_feature_df = load_data_frame(spark, prediction_feature_path + f"/cd={DATE}")\
        .select('request_id', 'match_id', 'content_id', 'date', 'tournament')\
        .cache()
    # load avg dau data
    estimated_dau_df = load_data_frame(spark, f'{dau_prediction_path}cd={DATE}/')\
        .withColumn('estimated_free_num', F.expr('DAU - subs_DAU'))\
        .selectExpr('cd as date', 'estimated_free_num', 'subs_DAU as estimated_sub_num')\
        .join(prediction_feature_df.select('date', 'tournament').distinct(), 'date')\
        .groupBy('tournament')\
        .agg(F.avg('estimated_free_num').alias('estimated_free_num'),
             F.avg('estimated_sub_num').alias('estimated_sub_num'))\
        .cache()
    prediction_feature_with_dau_df = prediction_feature_df\
        .join(estimated_dau_df, 'tournament')
    return prediction_feature_with_dau_df


def main(DATE):
    prediction_feature_with_dau_df = load_dataset(DATE)
    label_path = f"{pipeline_base_path}/dnn_predictions/cd={DATE}"
    common_cols = ['content_id']
    partition_col = "request_id"
    # load parameters predicted by dnn models
    new_prediction_df = prediction_feature_with_dau_df \
        .join(load_data_frame(spark, f"{label_path}/label={free_rate_label}"), common_cols) \
        .join(load_data_frame(spark, f"{label_path}/label={free_wt_label}"), common_cols) \
        .join(load_data_frame(spark, f"{label_path}/label={sub_rate_label}"), common_cols) \
        .join(load_data_frame(spark, f"{label_path}/label={sub_wt_label}"), common_cols) \
        .cache()
    total_match_duration_in_minutes, number_of_ad_breaks, average_length_of_a_break_in_seconds = match_configuration
    res_df = new_prediction_df \
        .withColumn('estimated_avg_concurrency', F.expr(
        f'(estimated_free_num * estimated_frees_watching_match_rate * estimated_watch_time_per_free_per_match '
        f'+ estimated_sub_num * estimated_subscribers_watching_match_rate * estimated_watch_time_per_subscriber_per_match)/{total_match_duration_in_minutes}')) \
        .withColumn('estimated_inventory', F.expr(
        f'estimated_avg_concurrency * {retention_rate} * ({number_of_ad_breaks * average_length_of_a_break_in_seconds} / 10.0)')) \
        .withColumn('estimated_reach', F.expr(
        f"(estimated_free_num * estimated_frees_watching_match_rate / {free_pid_did_rate}) + (estimated_sub_num * estimated_subscribers_watching_match_rate / {sub_pid_did_rate})")) \
        .withColumn('estimated_inventory', F.expr('cast(estimated_inventory as bigint)')) \
        .withColumn('estimated_reach', F.expr('cast(estimated_reach as bigint)')) \
        .cache()
    save_data_frame(res_df, pipeline_base_path + f"/inventory_prediction/future_tournaments/cd={DATE}/", partition_col=partition_col)


if __name__ == '__main__':
    DATE = sys.argv[1]
    if check_s3_path_exist(f"{prediction_feature_path}/cd={DATE}/"):
        main(DATE)

