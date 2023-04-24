from .path import *
from .util import *
from .config import *


def load_labels(tournament, feature_df):
    if not check_s3_folder_exist(pipeline_base_path + f"/label/inventory/tournament={tournament}"):
        df = feature_df\
            .where(f"tournament='{tournament}'")\
            .select('date', 'content_id', 'title')\
            .withColumn('total_inventory', F.lit(2023))\
            .withColumn('total_pid_reach', F.lit(-1))\
            .withColumn('total_did_reach', F.lit(-1))
    else:
        df = load_data_frame(spark, pipeline_base_path + f"/label/inventory/tournament={tournament}")
    # date, content_id, title,
    # total_inventory, total_pid_reach, total_did_reach
    return df.select('date', 'content_id', 'total_inventory', 'total_pid_reach', 'total_did_reach')


def free_timer_wt(wt_list):
    if len(wt_list) == 1:
        return float(wt_list[0])
    else:
        jio_rate = 0.75
        wt_list = sorted(wt_list)
        return (1 - jio_rate) * wt_list[0] + jio_rate * wt_list[1]


def load_dataset(config):
    path_suffix = "/all_features_hots_format_and_simple_one_hot"
    all_feature_df = load_data_frame(spark, pipeline_base_path + path_suffix) \
        .withColumn('tag', F.lit(1)) \
        .cache()
    if config == {}:
        predict_feature_df = load_data_frame(spark, live_ads_inventory_forecasting_complete_feature_path + f"/{default_predict_tournament}/all_features_hots_format") \
            .cache()
    else:
        base_path_suffix = "/prediction/all_features_hots_format_and_simple_one_hot"
        predict_feature_df = load_data_frame(spark, pipeline_base_path + base_path_suffix + f"/cd={DATE}") \
            .cache()
    common_cols = list(set(all_feature_df.columns).intersection(set(predict_feature_df.columns)))
    all_feature_df = all_feature_df.select(*common_cols)\
        .union(predict_feature_df.select(*common_cols))\
        .withColumn('tag', F.lit(1))\
        .cache()
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
    print(estimated_dau_df.count())
    estimated_dau_df.orderBy('tournament').show(20, False)
    return all_feature_df, estimated_dau_df


def main(mask_tag, config={}):
    all_feature_df, estimated_dau_df = load_dataset(config)
    if config == {}:
        test_tournaments = ["wc2019", "wc2021", "ipl2022", "ac2022", "wc2022", "wc2023"]
        parameter_path = ""
    else:
        test_tournaments = []
        for tournament in config:
            test_tournaments.append(tournament['seasonName'].replace(" ", "_").lower())
        parameter_path = f"future_tournaments/cd={DATE}/"
    res_list = []
    for test_tournament in test_tournaments:
        test_feature_df = all_feature_df \
            .where(f"tournament='{test_tournament}'") \
            .selectExpr('content_id', 'title', 'rank', 'teams', 'tournament', 'match_stage',
                        'total_frees_number', 'active_frees_rate as real_active_frees_rate',
                        'frees_watching_match_rate as real_frees_watching_match_rate',
                        'watch_time_per_free_per_match as real_watch_time_per_free_per_match',
                        'total_subscribers_number', 'active_subscribers_rate as real_active_subscribers_rate',
                        'subscribers_watching_match_rate as real_subscribers_watching_match_rate',
                        'watch_time_per_subscriber_per_match as real_watch_time_per_subscriber_per_match') \
            .cache()
        test_label_df = load_labels(f"{test_tournament}", all_feature_df) \
            .join(test_feature_df, 'content_id') \
            .cache()
        print(test_tournament)
        test_df = test_label_df \
            .join(estimated_dau_df, 'tournament') \
            .cache()
        label_cols = ['frees_watching_match_rate', "watch_time_per_free_per_match",
                      'subscribers_watching_match_rate', "watch_time_per_subscriber_per_match"]
        label_path = f"{pipeline_base_path}/xgb_prediction{mask_tag}/{parameter_path}tournament={test_tournament}"
        # print(first_match_date)
        new_test_label_df = test_df \
            .withColumn('estimated_variables', F.lit(0)) \
            .join(load_data_frame(spark, f"{label_path}/label={label_cols[0]}")
                .drop('sample_tag', 'real_' + label_cols[0]), ['date', 'content_id']) \
            .join(load_data_frame(spark, f"{label_path}/label={label_cols[2]}")
                .drop('sample_tag', 'real_' + label_cols[2]), ['date', 'content_id']) \
            .join(load_data_frame(spark, f"{label_path}/label={label_cols[3]}")
                .drop('sample_tag', 'real_' + label_cols[3]), ['date', 'content_id']) \
            .cache()
        label = 'watch_time_per_free_per_match_with_free_timer'
        parameter_df = load_data_frame(spark, f"{label_path}/label={label}") \
            .drop('sample_tag', 'real_' + label) \
            .groupBy('date', 'content_id') \
            .agg(F.collect_list('estimated_watch_time_per_free_per_match_with_free_timer').alias('estimated_watch_time_per_free_per_match')) \
            .withColumn('estimated_watch_time_per_free_per_match', free_timer_wt_udf('estimated_watch_time_per_free_per_match')) \
            .cache()
        new_test_label_df = new_test_label_df \
            .join(parameter_df, ['date', 'content_id']) \
            .cache()
        for configuration in duration_configurations[1:2]:
            total_match_duration_in_minutes, number_of_ad_breaks, average_length_of_a_break_in_seconds = configuration
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
                .withColumn('avg_concurrency_bias',
                            F.expr('(estimated_avg_concurrency - real_avg_concurrency) / real_avg_concurrency')) \
                .withColumn('reach_bias', F.expr('(estimated_reach - total_did_reach) / total_did_reach')) \
                .withColumn('reach_bias_abs', F.expr('abs(reach_bias)')) \
                .withColumn('inventory_bias', F.expr('(estimated_inventory - total_inventory) / total_inventory')) \
                .withColumn('inventory_bias_abs', F.expr('abs(estimated_inventory - total_inventory)')) \
                .withColumn('inventory_bias_abs_rate', F.expr('inventory_bias_abs / total_inventory')) \
                .where('total_inventory > 0') \
                .drop('teams') \
                .cache()
            cols = res_df.columns
            important_cols = ["real_avg_concurrency", "estimated_avg_concurrency", "avg_concurrency_bias",
                              "total_did_reach", 'estimated_reach', "reach_bias",
                              "total_inventory", "estimated_inventory", "inventory_bias", 'inventory_bias_abs']
            for col in important_cols:
                cols.remove(col)
            final_cols = cols + important_cols
            print(final_cols)
            res_df = res_df.select(*final_cols).orderBy('date', 'content_id')
            save_data_frame(res_df, pipeline_base_path + f"/inventory_prediction{mask_tag}/{parameter_path}test_tournament={test_tournament}")
            res_list.append(res_df.withColumn('tournament', F.lit(test_tournament)))
        print("")
        print("")
    return res_list


free_timer_wt_udf = F.udf(free_timer_wt, FloatType())


mask_tag = ""
# mask_tag = "_mask_knock_off"
DATE=sys.argv[1]
config = load_requests(DATE)
res_list = main(mask_tag=mask_tag, config=config)
tournament_dic = {
    "wc2023": -2,
    "ac2023": -1,
    "wc2022": 0,
    "ac2022": 1,
    "ipl2022": 2,
    "wc2021": 3,
    "wc2019": 4,
}
tag_mapping_udf = F.udf(lambda x: tournament_dic[x] if x in tournament_dic else 0, IntegerType())
reduce(lambda x, y: x.union(y), res_list) \
    .groupBy('tournament') \
    .agg(F.sum('real_avg_concurrency').alias('real_avg_concurrency'),
         F.sum('estimated_avg_concurrency').alias('estimated_avg_concurrency'),
         F.sum('total_inventory').alias('total_inventory'),
         F.sum('estimated_inventory').alias('estimated_inventory'),
         F.avg('inventory_bias_abs_rate').alias('avg_match_error'),
         F.sum('inventory_bias_abs').alias('sum_inventory_abs_error'),
         F.avg('reach_bias_abs').alias('avg_reach_bias_abs'),
         F.count('content_id')) \
    .withColumn('total_error', F.expr('(estimated_inventory - total_inventory) / total_inventory')) \
    .withColumn('total_match_error', F.expr('sum_inventory_abs_error / total_inventory')) \
    .withColumn('tag', tag_mapping_udf('tournament')) \
    .orderBy('tag')\
    .drop('tag')\
    .show(100, False)


