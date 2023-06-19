from path import *
from util import *
from config import *


def load_dataset(DATE):
    # load features
    predict_feature_df = load_data_frame(spark, pipeline_base_path + f"/prediction/all_features_hots_format/cd={DATE}")\
        .select('request_id', 'match_id', 'content_id', 'date', 'tournament')\
        .cache()
    # load avg dau data
    estimated_dau_df = load_data_frame(spark, f'{dau_prediction_path}cd={DATE}/')\
        .withColumn('estimated_free_num', F.expr('DAU - subs_DAU'))\
        .selectExpr('cd as date', 'estimated_free_num', 'subs_DAU as estimated_sub_num')\
        .join(predict_feature_df.select('date', 'tournament').distinct(), 'date')\
        .groupBy('tournament')\
        .agg(F.avg('estimated_free_num').alias('estimated_free_num'),
             F.avg('estimated_sub_num').alias('estimated_sub_num'))\
        .cache()
    return predict_feature_df.join(estimated_dau_df, 'tournament')


def main(DATE):
    prediction_df = load_dataset(DATE)
    label_cols = ['frees_watching_match_rate', "watch_time_per_free_per_match",
                  'subscribers_watching_match_rate', "watch_time_per_subscriber_per_match"]
    label_path = f"{pipeline_base_path}/dnn_predictions/cd={DATE}"
    common_cols = ['content_id']
    partition_col = "request_id"
    # load parameters predicted by dnn models
    new_prediction_df = prediction_df \
        .join(load_data_frame(spark, f"{label_path}/label={label_cols[0]}"), common_cols) \
        .join(load_data_frame(spark, f"{label_path}/label={label_cols[1]}"), common_cols) \
        .join(load_data_frame(spark, f"{label_path}/label={label_cols[2]}"), common_cols) \
        .join(load_data_frame(spark, f"{label_path}/label={label_cols[3]}"), common_cols) \
        .cache()
    total_match_duration_in_minutes, number_of_ad_breaks, average_length_of_a_break_in_seconds = match_configuration
    res_df = new_prediction_df \
        .withColumn('estimated_avg_concurrency', F.expr(
        f'(estimated_free_num * estimated_frees_watching_match_rate * estimated_watch_time_per_free_per_match '
        f'+ estimated_sub_num * estimated_subscribers_watching_match_rate * estimated_watch_time_per_subscriber_per_match)/{total_match_duration_in_minutes}')) \
        .withColumn('estimated_inventory', F.expr(
        f'estimated_avg_concurrency * {drop_off_rate} * ({number_of_ad_breaks * average_length_of_a_break_in_seconds} / 10.0)')) \
        .withColumn('estimated_reach', F.expr(
        f"(estimated_free_num * estimated_frees_watching_match_rate / {free_pid_did_rate}) + (estimated_sub_num * estimated_subscribers_watching_match_rate / {sub_pid_did_rate})")) \
        .withColumn('estimated_inventory', F.expr('cast(estimated_inventory as bigint)')) \
        .withColumn('estimated_reach', F.expr('cast(estimated_reach as bigint)')) \
        .cache()
    save_data_frame(res_df, pipeline_base_path + f"/inventory_prediction/future_tournaments/cd={DATE}/", partition_col=partition_col)


if __name__ == '__main__':
    DATE = sys.argv[1]
    config = load_requests(DATE)
    if config != {}:
        main(DATE)

