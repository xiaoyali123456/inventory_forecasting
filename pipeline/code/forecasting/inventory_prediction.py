from path import *
from util import *
from config import *


def load_labels():
    df = load_data_frame(spark, pipeline_base_path + f"/label/inventory")
    return df.select('content_id', 'total_inventory', 'total_pid_reach', 'total_did_reach')


def free_timer_wt(wt_list):
    if len(wt_list) == 1:
        return float(wt_list[0])
    else:
        # for tournaments like wc2019 with multiple free timers for different types of users
        wt_list = sorted(wt_list)
        none_jio_wt = wt_list[0]
        jio_wt = wt_list[1]
        return (1 - jio_user_rate_of_wc2019) * none_jio_wt + jio_user_rate_of_wc2019 * jio_wt


def load_dataset(config):
    path_suffix = "/all_features_hots_format_and_simple_one_hot"
    all_feature_df = load_data_frame(spark, pipeline_base_path + path_suffix) \
        .withColumn('request_id', F.lit(meaningless_request_id)) \
        .cache()
    if config == {}:
        predict_feature_df = reduce(lambda x, y: x.union(y), [load_data_frame(spark, live_ads_inventory_forecasting_complete_feature_path + f"/{predict_tournament}/all_features_hots_format")
                                                              for predict_tournament in xgb_configuration['predict_tournaments_candidate']]) \
            .withColumn('request_id', F.lit(meaningless_request_id)) \
            .cache()
    else:
        base_path_suffix = f"/prediction{path_suffix}"
        predict_feature_df = load_data_frame(spark, pipeline_base_path + base_path_suffix + f"/cd={DATE}") \
            .cache()
    common_cols = list(set(all_feature_df.columns).intersection(set(predict_feature_df.columns)))
    all_feature_df = all_feature_df.select(*common_cols)\
        .union(predict_feature_df.select(*common_cols))\
        .cache()
    # load avg dau data
    estimated_dau_df = all_feature_df\
        .selectExpr('tournament', 'total_frees_number as estimated_free_num', 'total_subscribers_number as estimated_sub_num')\
        .distinct()\
        .where(f'estimated_free_num > 0 and estimated_sub_num > 0')\
        .union(load_data_frame(spark, f'{dau_prediction_path}cd={DATE}/')
               .withColumn('estimated_free_num', F.expr('DAU - subs_DAU'))
               .selectExpr('cd as date', 'estimated_free_num', 'subs_DAU as estimated_sub_num')
               .join(predict_feature_df.select('date', 'tournament').distinct(), 'date')
               .groupBy('tournament')
               .agg(F.avg('estimated_free_num').alias('estimated_free_num'),
                    F.avg('estimated_sub_num').alias('estimated_sub_num'))
               .selectExpr('tournament', 'estimated_free_num', 'estimated_sub_num'))\
        .cache()
    return all_feature_df, estimated_dau_df


def main(mask_tag, config):
    all_feature_df, estimated_dau_df = load_dataset(config)
    if config == {}:
        # inventory forecasting for existing matches
        filter_operator = "="
        parameter_path = "previous_tournaments/"
        partition_col = "tournament"
    else:
        # inventory forecasting for new matches from api request
        filter_operator = "!="
        parameter_path = f"future_tournaments/cd={DATE}/"
        partition_col = "request_id"
    test_df = all_feature_df \
        .where(f"request_id {filter_operator} '{meaningless_request_id}'") \
        .selectExpr('request_id', 'date', 'content_id', 'title', 'rank', 'teams', 'tournament', 'vod_type',
                    'total_frees_number',
                    'frees_watching_match_rate as real_frees_watching_match_rate',
                    'watch_time_per_free_per_match as real_watch_time_per_free_per_match',
                    'total_subscribers_number',
                    'subscribers_watching_match_rate as real_subscribers_watching_match_rate',
                    'watch_time_per_subscriber_per_match as real_watch_time_per_subscriber_per_match') \
        .cache()
    test_df = test_df \
        .join(load_labels(), 'content_id', 'left') \
        .fillna(1, ['total_inventory', 'total_pid_reach', 'total_did_reach'])\
        .join(estimated_dau_df, 'tournament') \
        .cache()
    label_cols = ['frees_watching_match_rate', "watch_time_per_free_per_match",
                  'subscribers_watching_match_rate', "watch_time_per_subscriber_per_match"]
    label_path = f"{pipeline_base_path}/xgb_prediction{mask_tag}/{parameter_path}"
    svod_label_path = f"{pipeline_base_path}/xgb_prediction{mask_tag}_svod/{parameter_path}"
    useless_cols = ['tournament', 'sample_tag']
    common_cols = ['date', 'content_id']
    # load parameters predicted 2 sub-related variables by xgb models
    new_test_label_df = test_df \
        .join(load_data_frame(spark, f"{label_path}/label={label_cols[2]}").drop(*useless_cols, 'real_' + label_cols[2]), common_cols) \
        .join(load_data_frame(spark, f"{label_path}/label={label_cols[3]}").drop(*useless_cols, 'real_' + label_cols[3]), common_cols) \
        .cache()
    # load and calculate predicted frees_watching_match_rate by xgb models
    svod_free_rate_df = load_data_frame(spark, f"{svod_label_path}/label={label_cols[0]}") \
        .drop(*useless_cols, 'real_' + label_cols[0]) \
        .selectExpr(*common_cols, f'estimated_{label_cols[0]} as svod_rate')
    mix_free_rate_df = load_data_frame(spark, f"{label_path}/label={label_cols[0]}") \
        .drop(*useless_cols, 'real_' + label_cols[0]) \
        .selectExpr(*common_cols, f'estimated_{label_cols[0]} as mix_rate')
    new_test_label_df = new_test_label_df \
        .join(svod_free_rate_df
              .join(mix_free_rate_df, common_cols)
              .withColumn('avod_rate', F.expr(f'(mix_rate - {1 - jio_user_rate_of_wc2019} * svod_rate)/{jio_user_rate_of_wc2019}'))
              .drop('mix_rate'),
              common_cols) \
        .withColumn(f'estimated_{label_cols[0]}', F.expr('if(vod_type="avod", avod_rate, svod_rate)'))\
        .cache()
    # load and calculate predicted watch_time_per_free_per_match considering free timer
    label = 'watch_time_per_free_per_match_with_free_timer'
    parameter_df = load_data_frame(spark, f"{label_path}/label={label}") \
        .groupBy(common_cols) \
        .agg(F.collect_list('estimated_watch_time_per_free_per_match_with_free_timer').alias('estimated_watch_time_per_free_per_match')) \
        .withColumn('estimated_watch_time_per_free_per_match', free_timer_wt_udf('estimated_watch_time_per_free_per_match')) \
        .cache()
    new_test_label_df = new_test_label_df \
        .join(parameter_df, common_cols) \
        .cache()
    total_match_duration_in_minutes, number_of_ad_breaks, average_length_of_a_break_in_seconds = match_configuration
    res_df = new_test_label_df \
        .withColumn('real_avg_concurrency', F.expr(
        f'(total_frees_number * real_frees_watching_match_rate * real_watch_time_per_free_per_match '
        f'+ total_subscribers_number * real_subscribers_watching_match_rate * real_watch_time_per_subscriber_per_match)'
        f'/{total_match_duration_in_minutes}')) \
        .withColumn('estimated_avg_concurrency', F.expr(
        f'(estimated_free_num * estimated_frees_watching_match_rate * estimated_watch_time_per_free_per_match '
        f'+ estimated_sub_num * estimated_subscribers_watching_match_rate * estimated_watch_time_per_subscriber_per_match)/{total_match_duration_in_minutes}')) \
        .withColumn('estimated_inventory', F.expr(
        f'estimated_avg_concurrency * {drop_off_rate} * ({number_of_ad_breaks * average_length_of_a_break_in_seconds} / 10.0)')) \
        .withColumn('estimated_reach', F.expr(
        f"(estimated_free_num * estimated_frees_watching_match_rate / {free_pid_did_rate}) + (estimated_sub_num * estimated_subscribers_watching_match_rate / {sub_pid_did_rate})")) \
        .withColumn('estimated_inventory', F.expr('cast(estimated_inventory as bigint)')) \
        .withColumn('estimated_reach', F.expr('cast(estimated_reach as bigint)')) \
        .withColumn('reach_bias', F.expr('(estimated_reach - total_did_reach) / total_did_reach')) \
        .withColumn('reach_bias_abs', F.expr('abs(reach_bias)')) \
        .withColumn('inventory_bias', F.expr('(estimated_inventory - total_inventory) / total_inventory')) \
        .withColumn('inventory_bias_abs', F.expr('abs(estimated_inventory - total_inventory)')) \
        .withColumn('inventory_bias_abs_rate', F.expr('inventory_bias_abs / total_inventory')) \
        .where('total_inventory > 0') \
        .drop('teams')\
        .orderBy('date', 'content_id') \
        .cache()
    save_data_frame(res_df, pipeline_base_path + f"/inventory_prediction{mask_tag}/{parameter_path}", partition_col=partition_col)


free_timer_wt_udf = F.udf(free_timer_wt, FloatType())

if __name__ == '__main__':
    mask_tag = ""
    # mask_tag = "_mask_knock_off"
    DATE = sys.argv[1]
    config = load_requests(DATE)
    main(mask_tag=mask_tag, config=config)

