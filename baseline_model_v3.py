import datetime
import os
import sys
import time
import pickle
from functools import reduce
from math import log
import itertools
import math
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.shell import spark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
from sklearn.metrics.pairwise import cosine_similarity

storageLevel = StorageLevel.DISK_ONLY


def check_s3_path_exist(s3_path: str) -> bool:
    if not s3_path.endswith("/"):
        s3_path += "/"
    return os.system(f"aws s3 ls {s3_path}_SUCCESS") == 0


def get_date_list(date: str, days: int) -> list:
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    if -1 <= days <= 1:
        return [date]
    elif days > 1:
        return [(dt + datetime.timedelta(days=n)).strftime('%Y-%m-%d') for n in range(0, days)]
    else:
        return [(dt + datetime.timedelta(days=n)).strftime('%Y-%m-%d') for n in range(days + 1, 1)]


def load_data_frame(spark: SparkSession, path: str, fmt: str = 'parquet', header: bool = False, delimiter: str = ','
                    ) -> DataFrame:
    if fmt == 'parquet':
        return spark.read.parquet(path)
    elif fmt == 'orc':
        return spark.read.orc(path)
    elif fmt == 'jsond':
        return spark.read.json(path)
    elif fmt == 'csv':
        return spark.read.option('header', header).option('delimiter', delimiter).csv(path)
    else:
        print("the format is not supported")
        return DataFrame(None, None)


def hive_spark(name: str) -> SparkSession:
    return SparkSession.builder \
        .appName(name) \
        .config("hive.metastore.uris", "thrift://metastore.data.hotstar-labs.com:9083") \
        .config("spark.kryoserializer.buffer.max", "128m") \
        .enableHiveSupport() \
        .getOrCreate()


def load_hive_table(spark: SparkSession, table: str, date: str = None) -> DataFrame:
    if date is None:
        return spark.sql(f'select * from {table}')
    else:
        return spark.sql(f'select * from {table} where cd = "{date}"')


def save_data_frame(df: DataFrame, path: str, fmt: str = 'parquet', header: bool = False, delimiter: str = ',') -> None:
    def save_data_frame_internal(df: DataFrame, path: str, fmt: str = 'parquet', header: bool = False,
                                 delimiter: str = ',') -> None:
        if fmt == 'parquet':
            df.write.mode('overwrite').parquet(path)
        elif fmt == 'parquet2':
            df.write.mode('overwrite').parquet(path, compression='gzip')
        elif fmt == 'parquet3':
            df.write.mode('overwrite').parquet(path, compression='uncompressed')
        elif fmt == 'orc':
            df.write.mode('overwrite').orc(path)
        elif fmt == 'csv':
            df.coalesce(1).write.option('header', header).option('delimiter', delimiter).mode('overwrite').csv(path)
        elif fmt == 'csv_zip':
            df.write.option('header', header).option('delimiter', delimiter).option("compression", "gzip").mode(
                'overwrite').csv(path)
        else:
            print("the format is not supported")
    df.persist(storageLevel)
    try:
        save_data_frame_internal(df, path, fmt, header, delimiter)
    except Exception:
        try:
            save_data_frame_internal(df, path, 'parquet2', header, delimiter)
        except Exception:
            save_data_frame_internal(df, path, 'parquet3', header, delimiter)
    df.unpersist()


def check_title_valid(title, *args):
    for arg in args:
        if title.find(arg) > -1:
            return 0
    if title.find(' vs ') == -1:
        return 0
    return 1


def calculate_sub_num_on_target_date(sub_df, user_meta_df, target_date):
    sub_df = sub_df \
        .where(f'sub_start_time <= "{target_date}" and sub_end_time >= "{target_date}"') \
        .select('hid') \
        .distinct() \
        .join(user_meta_df, 'hid')
    return sub_df.select('dw_p_id').distinct()


def estimate_avg_concurrency(match_stage, teams, match_type,
                             active_frees_rate, frees_watching_match_rate, watch_time_per_free_per_match,
                             active_subscribers_rate, subscribers_watching_match_rate,
                             watch_time_per_subscriber_per_match,
                             avg_india_active_frees_rate, avg_india_frees_watching_match_rate,
                             avg_india_watch_time_per_free_per_match,
                             avg_india_active_subscribers_rate, avg_india_subscribers_watching_match_rate,
                             avg_india_watch_time_per_subscriber_per_match,
                             avg_stage_1_active_frees_rate, avg_stage_1_frees_watching_match_rate,
                             avg_stage_1_watch_time_per_free_per_match,
                             avg_stage_1_active_subscribers_rate, avg_stage_1_subscribers_watching_match_rate,
                             avg_stage_1_watch_time_per_subscriber_per_match,
                             avg_stage_2_active_frees_rate, avg_stage_2_frees_watching_match_rate,
                             avg_stage_2_watch_time_per_free_per_match,
                             avg_stage_2_active_subscribers_rate, avg_stage_2_subscribers_watching_match_rate,
                             avg_stage_2_watch_time_per_subscriber_per_match,
                             avg_sf_active_frees_rate, avg_sf_frees_watching_match_rate,
                             avg_sf_watch_time_per_free_per_match,
                             avg_sf_active_subscribers_rate, avg_sf_subscribers_watching_match_rate,
                             avg_sf_watch_time_per_subscriber_per_match,
                             avg_final_active_frees_rate, avg_final_frees_watching_match_rate,
                             avg_final_watch_time_per_free_per_match,
                             avg_final_active_subscribers_rate, avg_final_subscribers_watching_match_rate,
                             avg_final_watch_time_per_subscriber_per_match):
    if match_stage == "semi-final":
        free_rate = avg_sf_active_frees_rate
        free_watch_rate = avg_sf_frees_watching_match_rate
        free_watch_time = avg_sf_watch_time_per_free_per_match
        sub_rate = avg_sf_active_subscribers_rate
        sub_watch_rate = avg_sf_subscribers_watching_match_rate
        sub_watch_time = avg_sf_watch_time_per_subscriber_per_match
    elif match_stage == "final":
        free_rate = avg_final_active_frees_rate
        free_watch_rate = avg_final_frees_watching_match_rate
        free_watch_time = avg_final_watch_time_per_free_per_match
        sub_rate = avg_final_active_subscribers_rate
        sub_watch_rate = avg_final_subscribers_watching_match_rate
        sub_watch_time = avg_final_watch_time_per_subscriber_per_match
    elif active_frees_rate > 0:
        free_rate = active_frees_rate
        free_watch_rate = frees_watching_match_rate
        free_watch_time = watch_time_per_free_per_match
        sub_rate = active_subscribers_rate
        sub_watch_rate = subscribers_watching_match_rate
        sub_watch_time = watch_time_per_subscriber_per_match
    elif teams.find("india") > -1:
        free_rate = avg_india_active_frees_rate
        free_watch_rate = avg_india_frees_watching_match_rate
        free_watch_time = avg_india_watch_time_per_free_per_match
        sub_rate = avg_india_active_subscribers_rate
        sub_watch_rate = avg_india_subscribers_watching_match_rate
        sub_watch_time = avg_india_watch_time_per_subscriber_per_match
    elif match_stage == "qualifier":
        free_rate = avg_stage_1_active_frees_rate
        free_watch_rate = avg_stage_1_frees_watching_match_rate
        free_watch_time = avg_stage_1_watch_time_per_free_per_match
        sub_rate = avg_stage_1_active_subscribers_rate
        sub_watch_rate = avg_stage_1_subscribers_watching_match_rate
        sub_watch_time = avg_stage_1_watch_time_per_subscriber_per_match
    else:
        free_rate = avg_stage_2_active_frees_rate
        free_watch_rate = avg_stage_2_frees_watching_match_rate
        free_watch_time = avg_stage_2_watch_time_per_free_per_match
        sub_rate = avg_stage_2_active_subscribers_rate
        sub_watch_rate = avg_stage_2_subscribers_watching_match_rate
        sub_watch_time = avg_stage_2_watch_time_per_subscriber_per_match
    if match_stage.find("final") > -1 and teams.find("india") > -1:
        free_rate = min(1.0, free_rate + 0.1)
        free_watch_rate = min(1.0, free_watch_rate + 0.1)
        sub_rate = min(1.0, sub_rate + 0.1)
        sub_watch_rate = min(1.0, sub_watch_rate + 0.1)
        free_watch_time = free_watch_time * 1.5
        sub_watch_time = sub_watch_time * 1.5
    # free_watch_time = min(free_watch_time, 5.0)
    # avg_concurrency = ((float(total_free_num) * free_rate * free_watch_rate * free_watch_time)
    #                    + (float(total_sub_num) * sub_rate * sub_watch_rate * sub_watch_time))/total_match_duration_in_minutes
    res = [free_rate, free_watch_rate, free_watch_time, sub_rate, sub_watch_rate, sub_watch_time]
    return res
    # return float(avg_concurrency * 0.8 * (number_of_ad_breaks * average_length_of_a_break_in_seconds / 10.0))


# cosine_similarity([[1,1,2,1,1,1,0,0,0]],[[1,1,1,0,1,1,1,1,1]])[0][0]
def calculate_similarity_between_two_matches(target_feature, feature, feature_num, sub_version):
    similarity = 0.0
    if version == "baseline_with_feature_similarity":
        for i in range(feature_num):
            similarity += float(cosine_similarity([target_feature[i]], [feature[i]])[0][0])
        return similarity / feature_num
    else:
        for i in range(feature_num):
            similarity += feature_weights_list[sub_version][i][1] * float(
                cosine_similarity([target_feature[i]], [feature[i]])[0][0])
        return similarity


def estimate_avg_concurrency_using_feature_similarity(sub_version, *args):
    similarity_list = []
    for content_key in valid_feature_dic:
        similarity_list.append((content_key,
                                calculate_similarity_between_two_matches(args, valid_feature_dic[content_key],
                                                                         feature_num, sub_version)))
    final_similarity_list = sorted(similarity_list, key=lambda content_key: content_key[1], reverse=True)[
                            :top_N_matches]
    # print(final_similarity_list)
    res = [0.0 for i in range(dynamic_parameter_num)]
    similarity_sum = 0.0
    for content_key in final_similarity_list:
        similarity_sum += content_key[1]
    if similarity_sum == 0.0:
        similarity_sum = 1.0
    for content_key in final_similarity_list:
        res = list(map(lambda x: x[0] + x[1],
                       zip(res, [content_key[1] / similarity_sum * x for x in valid_parameter_dic[content_key[0]]])))
    return res


def load_wt_features(tournament):
    # df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/free_checking_result_of_{tournament}") \
    #     .join(load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/sub_checking_result_of_{tournament}")
    #           .withColumnRenamed('avg_watch_time', 'avg_sub_watch_time').withColumnRenamed('total_watch_time',
    #                                                                                        'total_sub_watch_time'),
    #           ['date', 'content_id', 'title', 'shortsummary']) \
    #     .withColumn("if_contain_india_team", F.locate('india', F.col('title'))) \
    #     .withColumn('if_contain_india_team', F.expr('if(if_contain_india_team > 0, 1, 0)')) \
    #     .withColumn('if_weekend', F.dayofweek(F.col('date'))) \
    #     .withColumn('if_weekend', F.expr('if(if_weekend=1 or if_weekend = 7, 1, 0)')) \
    #     .withColumnRenamed('sub_num', 'total_subscribers_number') \
    #     .withColumnRenamed('active_sub_rate', 'active_subscribers_rate') \
    #     .withColumnRenamed('match_active_sub_rate', 'subscribers_watching_match_rate') \
    #     .withColumnRenamed('avg_sub_watch_time', 'watch_time_per_subscriber_per_match') \
    #     .withColumnRenamed('free_num', 'total_frees_number') \
    #     .withColumnRenamed('active_free_rate', 'active_frees_rate') \
    #     .withColumnRenamed('match_active_free_rate', 'frees_watching_match_rate') \
    #     .withColumnRenamed('avg_watch_time', 'watch_time_per_free_per_match')
    # return df \
    #     .selectExpr('date', 'content_id', 'title', 'shortsummary',
    #                 'total_frees_number', 'active_free_num', 'match_active_free_num', 'total_free_watch_time',
    #                 'total_subscribers_number', 'active_sub_num', 'match_active_sub_num', 'total_sub_watch_time',
    #                 'active_frees_rate', 'frees_watching_match_rate', 'watch_time_per_free_per_match',
    #                 'active_subscribers_rate', 'subscribers_watching_match_rate', 'watch_time_per_subscriber_per_match',
    #                 'if_contain_india_team', 'if_weekend') \
    #     .orderBy('date', 'content_id') \
    #     .withColumn('tournament', F.lit(tournament))
    return load_data_frame(spark,
                           live_ads_inventory_forecasting_root_path + f"/baseline_features_final/all_features_hots_format") \
        .where(f'tournament="{tournament}"')


def load_labels(tournament, feature_df):
    data_source = "watched_video_sampled"
    if not check_s3_path_exist(live_ads_inventory_forecasting_root_path + f"/final_test_dataset/{data_source}_of_{tournament}"):
        data_source = "watched_video"
    if not check_s3_path_exist(live_ads_inventory_forecasting_root_path + f"/final_test_dataset/{data_source}_of_{tournament}"):
        df = feature_df\
            .where(f"tournament='{tournament}'")\
            .select('date', 'content_id', 'title', 'shortsummary')\
            .withColumn('total_inventory', F.lit(2023))\
            .withColumn('total_pid_reach', F.lit(-1))\
            .withColumn('total_did_reach', F.lit(-1))
    else:
        df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/final_test_dataset/{data_source}_of_{tournament}")
    # date, content_id, title, shortsummary,
    # total_inventory, total_pid_reach, total_did_reach
    return df


def first_match_data(df, col):
    min_rank = df.select(F.min('rank')).collect()[0][0]
    return df.where(f'rank = {min_rank}').select(col).collect()[0][0]


def last_match_data(df, col):
    max_rank = df.select(F.max('rank')).collect()[0][0]
    return df.where(f'rank = {max_rank}').select(col).collect()[0][0]


def days_bewteen_st_and_et(st, et):
    date1 = datetime.datetime.strptime(st, "%Y-%m-%d").date()
    date2 = datetime.datetime.strptime(et, "%Y-%m-%d").date()
    return (date2 - date1).days


def get_first_match_data_and_increasing_rate(previous_tournament, tag_tournament_1,
                                             tag_tournament_2, base_tournament,
                                             col, base_tournament_days, test_df):
    first_match_num = last_match_data(all_feature_df.where(f"tournament='{previous_tournament}'"), col) + \
                      ((first_match_data(all_feature_df.where(f"tournament='{tag_tournament_2}'"), col) -
                        last_match_data(all_feature_df.where(f"tournament='{tag_tournament_1}'"), col)) /
                       days_bewteen_st_and_et(
                           last_match_data(all_feature_df.where(f"tournament='{tag_tournament_1}'"), 'date'),
                           first_match_data(all_feature_df.where(f"tournament='{tag_tournament_2}'"), 'date'))) \
                      * days_bewteen_st_and_et(
        last_match_data(all_feature_df.where(f"tournament='{previous_tournament}'"), 'date'),
        first_match_data(test_df, 'date'))
    num_increasing_rate = (last_match_data(all_feature_df.where(f"tournament='{base_tournament}'"), col)
                           - first_match_data(all_feature_df.where(f"tournament='{base_tournament}'"),
                                              col)) / base_tournament_days
    return first_match_num, num_increasing_rate


check_title_valid_udf = F.udf(check_title_valid, IntegerType())
estimate_avg_concurrency_udf = F.udf(estimate_avg_concurrency, ArrayType(FloatType()))
estimate_avg_concurrency_using_feature_similarity_udf = F.udf(estimate_avg_concurrency_using_feature_similarity,
                                                              ArrayType(FloatType()))

concurrency_root_path = "s3://hotstar-dp-datalake-processed-us-east-1-prod/hive_internal_database/concurrency.db/"
ssai_concurrency_path = f"{concurrency_root_path}/users_by_live_sports_content_by_ssai"
match_meta_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/ads_crash/match_meta"
live_ads_inventory_forecasting_root_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting"
play_out_log_input_path = "s3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test"
watchAggregatedInputPath = "s3://hotstar-dp-datalake-processed-us-east-1-prod/aggregates/watched_video_daily_aggregates_ist_v4"
viewAggregatedInputPath = "s3://hotstar-dp-datalake-processed-us-east-1-prod/aggregates/viewed_page_daily_aggregates_ist_v2"
live_ads_inventory_forecasting_complete_feature_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/complete_features"

# total_match_duration_in_minutes, number_of_ad_breaks, average_length_of_a_break_in_seconds = 325.0, 55.0, 80.0
# total_match_duration_in_minutes, number_of_ad_breaks, average_length_of_a_break_in_seconds = 210.0, 85.0, 30.0
# total_match_duration_in_minutes, number_of_ad_breaks, average_length_of_a_break_in_seconds = 210.0, 45.0, 55.0
configurations = [(210.0, 55.0, 80.0), (210.0, 85.0, 30.0), (210.0, 45.0, 55.0)]
# drop_off_rate = 0.8
drop_off_rate = 0.85
dynamic_parameters = ['active_frees_rate', 'frees_watching_match_rate', "watch_time_per_free_per_match",
                      'active_subscribers_rate', 'subscribers_watching_match_rate',
                      "watch_time_per_subscriber_per_match"]
dynamic_parameter_num = len(dynamic_parameters)
one_hot_cols = ['tournament_type', 'if_weekend', 'match_time', 'if_holiday', 'venue', 'if_contain_india_team',
                'match_type', 'tournament_name', 'hostar_influence',
                'match_stage', 'vod_type', 'gender_type']
multi_hot_cols = ['teams', 'continents', 'teams_tier']
additional_cols = ["languages", "platforms"]
invalid_cols = ['gender_type']
feature_cols = [col + "_hot_vector" for col in one_hot_cols[:-1] + multi_hot_cols + additional_cols]
feature_num = len(feature_cols)
top_N_matches = 5
# test_tournament = "wc2022"
# test_tournament = "ac2022"
# test_tournament = "wc2021"
# test_tournament = "ipl2022"
# version = "baseline"
# version = "baseline_with_feature_similarity"
# version = "baseline_with_weighted_feature_similarity"
version = "baseline_with_predicted_parameters"
mask_tag = ""
# mask_tag = "mask_knock_off"
# version = "save_free_and_sub_number_predictions"
# sub_version = 3
feature_weights_list = [
    [('languages_hot_vector', 0.38048658163972215), ('match_stage_hot_vector', 0.3793939397957266),
     ('teams_hot_vector', 0.22036647224640366), ('if_holiday_hot_vector', 0.016830258770679535),
     ('hostar_influence_hot_vector', 0.002922747547468044), ('if_weekend_hot_vector', 0.0),
     ('match_time_hot_vector', 0.0), ('venue_hot_vector', 0.0), ('if_contain_india_team_hot_vector', 0.0),
     ('match_type_hot_vector', 0.0), ('tournament_name_hot_vector', 0.0), ('platforms_hot_vector', 0.0)],
    [('teams_hot_vector', 0.4729149281399665), ('match_stage_hot_vector', 0.20695029157027847),
     ('languages_hot_vector', 0.19703538722240327), ('if_weekend_hot_vector', 0.07543480673749854),
     ('match_time_hot_vector', 0.03896483033524315), ('if_holiday_hot_vector', 0.008699755994610073),
     ('venue_hot_vector', 0.0), ('if_contain_india_team_hot_vector', 0.0), ('match_type_hot_vector', 0.0),
     ('tournament_name_hot_vector', 0.0), ('hostar_influence_hot_vector', 0.0), ('platforms_hot_vector', 0.0)],
    [('teams_hot_vector', 0.5436064036781025), ('languages_hot_vector', 0.1517476180081545),
     ('match_stage_hot_vector', 0.11868934076557833), ('if_holiday_hot_vector', 0.08080482086971215),
     ('match_time_hot_vector', 0.07831964752560516), ('if_weekend_hot_vector', 0.0268321691528473),
     ('venue_hot_vector', 0.0), ('if_contain_india_team_hot_vector', 0.0), ('match_type_hot_vector', 0.0),
     ('tournament_name_hot_vector', 0.0), ('hostar_influence_hot_vector', 0.0), ('platforms_hot_vector', 0.0)],
    [('teams_hot_vector', 0.7113281485914227), ('match_time_hot_vector', 0.08665182675410503),
     ('if_weekend_hot_vector', 0.08254502272163539), ('match_stage_hot_vector', 0.061415377085369045),
     ('languages_hot_vector', 0.057461103457451916), ('if_holiday_hot_vector', 0.0005985213900158332),
     ('venue_hot_vector', 0.0), ('if_contain_india_team_hot_vector', 0.0), ('match_type_hot_vector', 0.0),
     ('tournament_name_hot_vector', 0.0), ('hostar_influence_hot_vector', 0.0), ('platforms_hot_vector', 0.0)]
]

predict_tournament = "wc2023"
dau_prediction_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_predict/DAU_predict.parquet"
# feature_weights = feature_weights_list[sub_version]
sub_pid_did_rate = 0.94
free_pid_did_rate = 1.02

# calculation for wc 2022
# total_free_number: first-match = first match of wc 2021, rest matches are increasing by rate in match level of wc 2021
# total_sub_number: first-match = last match of ipl 2022 + normal-increasing-rate-in-date-level * days-between-ipl-2022-and-wc-2022, rest matches are increasing by rate in match level of wc 2021
# wt-related-parameters: select most similar matches of wc 2021, and use the avg value of the parameters in those matches
#     order: knockout, contains india + 10%
# total_match_duration = X min, number_of_ad_breaks and average_length_of_a_break_in_seconds used the same as wc 2021
path_suffix = "/all_features_hots_format_with_avg_au_sub_free_num"
all_feature_df = load_data_frame(spark, live_ads_inventory_forecasting_complete_feature_path + path_suffix) \
    .withColumn('tag', F.lit(1)) \
    .cache()

# all_feature_df.where('tournament != "ipl2019"')\
#     .orderBy('date', 'content_id')\
#     .select('tournament', 'content_id', 'total_frees_number', 'frees_watching_match_rate', 'total_subscribers_number', 'subscribers_watching_match_rate')\
#     .show(500, False)

predict_feature_df = load_data_frame(spark, live_ads_inventory_forecasting_complete_feature_path + f"/{predict_tournament}/all_features_hots_format")\
    .cache()

common_cols = list(set(all_feature_df.columns).intersection(set(predict_feature_df.columns)))

all_feature_df = all_feature_df.select(*common_cols)\
    .union(predict_feature_df.select(*common_cols))\
    .withColumn('tag', F.lit(1))\
    .cache()

# if not check_s3_path_exist(live_ads_inventory_forecasting_complete_feature_path + "/dau_prediction"):
#     dau_df = load_data_frame(spark, dau_prediction_path) \
#         .withColumn('estimated_free_num', F.expr('DAU - subs_DAU')) \
#         .selectExpr('cd as date', 'estimated_free_num', 'subs_DAU as estimated_sub_num') \
#         .join(all_feature_df.select('date', 'tournament').distinct(), 'date') \
#         .groupBy('tournament') \
#         .agg(F.avg('estimated_free_num').alias('estimated_free_num'),
#              F.avg('estimated_sub_num').alias('estimated_sub_num'))
#     save_data_frame(dau_df, live_ads_inventory_forecasting_complete_feature_path + "/dau_prediction")

estimated_dau_df = all_feature_df\
    .selectExpr('tournament', 'total_frees_number as estimated_free_num', 'total_subscribers_number as estimated_sub_num')\
    .distinct()\
    .where(f'tournament != "{predict_tournament}"')\
    .union(load_data_frame(spark, dau_prediction_path)
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

feature_df = all_feature_df.where('tournament != "ipl2019"').cache()
tournament_df = all_feature_df \
    .groupBy('tournament') \
    .agg(F.min('date').alias('date')) \
    .orderBy('date') \
    .select('tournament') \
    .collect()
tournament_list = [item[0] for item in tournament_df]
print(tournament_list)
tournament_idx_dic = {}
match_type_list = ["t20", "odi", "test"]
matching_features_list = ['match_type', 'vod_type', 'tournament_type', 'tournament_name']
for idx in range(len(tournament_list)):
    tournament_idx_dic[tournament_list[idx]] = idx

res_list = []

# df = reduce(lambda x, y: x.union(y), [load_labels(tournament).withColumn('tournament', F.lit(tournament)) for tournament in tournament_list[1:]]).where('total_inventory > 0').cache()
# df.orderBy('date', 'content_id').show(1000, False)
# df.groupBy('tournament').agg(F.avg('total_inventory').alias('avg_inventory'), F.count('title')).orderBy('avg_inventory').show(1000, False)


# for test_tournament in tournament_list:
for test_tournament in ["wc2019", "wc2021", "ipl2022", "ac2022", "wc2022", "wc2023"]:
    # for test_tournament in ["ac2022", "wc2022"]:
    # for test_tournament in ["wc2019"]:
    # for test_tournament in ["wc2023"]:
    if test_tournament == "ipl2019":
        continue
    previous_tournament = tournament_list[tournament_idx_dic[test_tournament] - 1]
    if tournament_idx_dic[previous_tournament] == 0:
        tag_tournament_1 = tournament_list[2]
        tag_tournament_2 = tournament_list[3]
    else:
        tag_tournament_1 = tournament_list[tournament_idx_dic[previous_tournament] - 1]
        tag_tournament_2 = previous_tournament
    test_feature_df = feature_df \
        .where(f"tournament='{test_tournament}'") \
        .selectExpr('content_id', 'rank', 'teams', 'tournament', *matching_features_list, 'match_stage',
                    'total_frees_number', 'active_frees_rate as real_active_frees_rate',
                    'frees_watching_match_rate as real_frees_watching_match_rate',
                    'watch_time_per_free_per_match as real_watch_time_per_free_per_match',
                    'total_subscribers_number', 'active_subscribers_rate as real_active_subscribers_rate',
                    'subscribers_watching_match_rate as real_subscribers_watching_match_rate',
                    'watch_time_per_subscriber_per_match as real_watch_time_per_subscriber_per_match') \
        .cache()
    test_match_type_list = test_feature_df.select('match_type').distinct().collect()
    # print(test_match_type_list)
    test_label_df = load_labels(f"{test_tournament}", all_feature_df) \
        .join(test_feature_df, 'content_id') \
        .cache()
    simple_feature_df = feature_df \
        .where(f"tournament!='{test_tournament}'") \
        .select('tournament', *matching_features_list) \
        .distinct() \
        .cache()
    for match_type in test_match_type_list:
        print(test_tournament)
        print(match_type[0])
        test_df = test_label_df \
            .where(f'match_type="{match_type[0]}"') \
            .cache()
        simple_test_df = test_df \
            .select(*matching_features_list) \
            .distinct() \
            .cache()
        for i in range(len(matching_features_list), 0, -1):
            if simple_test_df.join(simple_feature_df, matching_features_list[:i]).count() > 0:
                base_tournament = simple_test_df \
                    .join(simple_feature_df, matching_features_list[:i]) \
                    .select('tournament') \
                    .distinct() \
                    .collect()[0][0]
                break
        print(base_tournament)
        # print("")
        base_tournament_days = days_bewteen_st_and_et(
            first_match_data(feature_df.where(f"tournament='{base_tournament}'"), 'date'),
            last_match_data(feature_df.where(f"tournament='{base_tournament}'"), 'date'))
        if base_tournament == "ipl2021":
            base_tournament_days -= days_bewteen_st_and_et("2021-05-03", "2021-09-19")
        first_match_sub_num, sub_num_increasing_rate = get_first_match_data_and_increasing_rate(previous_tournament,
                                                                                                tag_tournament_1,
                                                                                                tag_tournament_2,
                                                                                                base_tournament,
                                                                                                "total_subscribers_number",
                                                                                                base_tournament_days,
                                                                                                test_df)
        first_match_free_num, free_num_increasing_rate = get_first_match_data_and_increasing_rate(previous_tournament,
                                                                                                  tag_tournament_1,
                                                                                                  tag_tournament_2,
                                                                                                  base_tournament,
                                                                                                  "total_frees_number",
                                                                                                  base_tournament_days,
                                                                                                  test_df)
        first_match_date = first_match_data(test_df, 'date')
        # print(first_match_date)
        # print(base_tournament_days)
        # print(first_match_free_num)
        # print(free_num_increasing_rate)
        # print(first_match_sub_num)
        # print(sub_num_increasing_rate)
        # test_df = test_df\
        #     .withColumn('datediff', F.datediff(F.col('date'), F.lit(first_match_date))) \
        #     .withColumn('estimated_free_num', F.expr(f"{first_match_free_num} + datediff * {free_num_increasing_rate}")) \
        #     .withColumn('estimated_sub_num', F.expr(f"{first_match_sub_num} + datediff * {sub_num_increasing_rate}")) \
        #     .cache()
        test_df = test_df \
            .join(estimated_dau_df, 'tournament') \
            .cache()
        # <-- for raw baseline with simple rules -->
        if version == "baseline":
            base_tournament_feature_df = feature_df \
                .where(f"tournament='{base_tournament}' and match_type='{match_type[0]}'") \
                .cache()
            india_match_df = base_tournament_feature_df \
                .where(
                f'(if_contain_india_team = 1 and tournament_type = "international") or (tournament_type != "international")') \
                .where(f'match_stage = "group"') \
                .groupBy('tag') \
                .agg(F.avg('active_frees_rate').alias('avg_india_active_frees_rate'),
                     F.avg('frees_watching_match_rate').alias('avg_india_frees_watching_match_rate'),
                     F.avg('watch_time_per_free_per_match').alias('avg_india_watch_time_per_free_per_match'),
                     F.avg('active_subscribers_rate').alias('avg_india_active_subscribers_rate'),
                     F.avg('subscribers_watching_match_rate').alias('avg_india_subscribers_watching_match_rate'),
                     F.avg('watch_time_per_subscriber_per_match').alias(
                         'avg_india_watch_time_per_subscriber_per_match')) \
                .cache()
            # print(india_match_df.columns)
            group_match_df = base_tournament_feature_df \
                .where(
                f'(if_contain_india_team = 0 and tournament_type = "international") or (tournament_type != "international")') \
                .where(f'match_stage = "group"') \
                .groupBy('tag') \
                .agg(F.avg('active_frees_rate').alias('avg_stage_2_active_frees_rate'),
                     F.avg('frees_watching_match_rate').alias('avg_stage_2_frees_watching_match_rate'),
                     F.avg('watch_time_per_free_per_match').alias('avg_stage_2_watch_time_per_free_per_match'),
                     F.avg('active_subscribers_rate').alias('avg_stage_2_active_subscribers_rate'),
                     F.avg('subscribers_watching_match_rate').alias('avg_stage_2_subscribers_watching_match_rate'),
                     F.avg('watch_time_per_subscriber_per_match').alias(
                         'avg_stage_2_watch_time_per_subscriber_per_match')) \
                .cache()
            qualifier_match_df = base_tournament_feature_df \
                .where(
                f'(if_contain_india_team = 0 and tournament_type = "international") or (tournament_type != "international")') \
                .where(f'match_stage = "qualifier"') \
                .groupBy('tag') \
                .agg(F.avg('active_frees_rate').alias('avg_stage_1_active_frees_rate'),
                     F.avg('frees_watching_match_rate').alias('avg_stage_1_frees_watching_match_rate'),
                     F.avg('watch_time_per_free_per_match').alias('avg_stage_1_watch_time_per_free_per_match'),
                     F.avg('active_subscribers_rate').alias('avg_stage_1_active_subscribers_rate'),
                     F.avg('subscribers_watching_match_rate').alias('avg_stage_1_subscribers_watching_match_rate'),
                     F.avg('watch_time_per_subscriber_per_match').alias(
                         'avg_stage_1_watch_time_per_subscriber_per_match')) \
                .cache()
            # print(stage_1_match_df.columns)
            sf_match_df = base_tournament_feature_df \
                .where('match_stage = "semi-final"') \
                .groupBy('tag') \
                .agg(F.avg('active_frees_rate').alias('avg_sf_active_frees_rate'),
                     F.avg('frees_watching_match_rate').alias('avg_sf_frees_watching_match_rate'),
                     F.avg('watch_time_per_free_per_match').alias('avg_sf_watch_time_per_free_per_match'),
                     F.avg('active_subscribers_rate').alias('avg_sf_active_subscribers_rate'),
                     F.avg('subscribers_watching_match_rate').alias('avg_sf_subscribers_watching_match_rate'),
                     F.avg('watch_time_per_subscriber_per_match').alias('avg_sf_watch_time_per_subscriber_per_match')) \
                .cache()
            # print(sf_match_df.columns)
            final_match_df = base_tournament_feature_df \
                .where(f'match_stage = "final"') \
                .groupBy('tag') \
                .agg(F.avg('active_frees_rate').alias('avg_final_active_frees_rate'),
                     F.avg('frees_watching_match_rate').alias('avg_final_frees_watching_match_rate'),
                     F.avg('watch_time_per_free_per_match').alias('avg_final_watch_time_per_free_per_match'),
                     F.avg('active_subscribers_rate').alias('avg_final_active_subscribers_rate'),
                     F.avg('subscribers_watching_match_rate').alias('avg_final_subscribers_watching_match_rate'),
                     F.avg('watch_time_per_subscriber_per_match').alias(
                         'avg_final_watch_time_per_subscriber_per_match')) \
                .cache()
            if qualifier_match_df.count() == 0:
                qualifier_match_df = feature_df \
                    .where(f"match_stage = 'qualifier' and tournament!='{test_tournament}'") \
                    .groupBy('tag') \
                    .agg(F.avg('active_frees_rate').alias('avg_stage_1_active_frees_rate'),
                         F.avg('frees_watching_match_rate').alias('avg_stage_1_frees_watching_match_rate'),
                         F.avg('watch_time_per_free_per_match').alias('avg_stage_1_watch_time_per_free_per_match'),
                         F.avg('active_subscribers_rate').alias('avg_stage_1_active_subscribers_rate'),
                         F.avg('subscribers_watching_match_rate').alias('avg_stage_1_subscribers_watching_match_rate'),
                         F.avg('watch_time_per_subscriber_per_match').alias(
                             'avg_stage_1_watch_time_per_subscriber_per_match')) \
                    .cache()
            if sf_match_df.count() == 0:
                sf_match_df = feature_df \
                    .where(f'match_stage = "semi-final" and tournament!="{test_tournament}"') \
                    .groupBy('tag') \
                    .agg(F.avg('active_frees_rate').alias('avg_sf_active_frees_rate'),
                         F.avg('frees_watching_match_rate').alias('avg_sf_frees_watching_match_rate'),
                         F.avg('watch_time_per_free_per_match').alias('avg_sf_watch_time_per_free_per_match'),
                         F.avg('active_subscribers_rate').alias('avg_sf_active_subscribers_rate'),
                         F.avg('subscribers_watching_match_rate').alias('avg_sf_subscribers_watching_match_rate'),
                         F.avg('watch_time_per_subscriber_per_match').alias(
                             'avg_sf_watch_time_per_subscriber_per_match')) \
                    .cache()
            if final_match_df.count() == 0:
                final_match_df = feature_df \
                    .where(f'match_stage = "final" and tournament!="{test_tournament}"') \
                    .groupBy('tag') \
                    .agg(F.avg('active_frees_rate').alias('avg_final_active_frees_rate'),
                         F.avg('frees_watching_match_rate').alias('avg_final_frees_watching_match_rate'),
                         F.avg('watch_time_per_free_per_match').alias('avg_final_watch_time_per_free_per_match'),
                         F.avg('active_subscribers_rate').alias('avg_final_active_subscribers_rate'),
                         F.avg('subscribers_watching_match_rate').alias('avg_final_subscribers_watching_match_rate'),
                         F.avg('watch_time_per_subscriber_per_match').alias(
                             'avg_final_watch_time_per_subscriber_per_match')) \
                    .cache()
            # print(final_match_df.columns)
            # avg_concurrency = ((float(total_free_num) * free_rate * free_watch_rate * free_watch_time)
            #                        + (float(total_sub_num) * sub_rate * sub_watch_rate * sub_watch_time))/total_match_duration_in_minutes
            # res = [free_rate, free_watch_rate, free_watch_time, sub_rate, sub_watch_rate, sub_watch_time]
            # print(test_df.count())
            new_test_label_df = test_df \
                .crossJoin(F.broadcast(india_match_df.drop('tag'))) \
                .crossJoin(F.broadcast(qualifier_match_df.drop('tag'))) \
                .crossJoin(F.broadcast(group_match_df.drop('tag'))) \
                .crossJoin(F.broadcast(sf_match_df.drop('tag'))) \
                .crossJoin(F.broadcast(final_match_df.drop('tag'))) \
                .join(base_tournament_feature_df.where(f'match_stage = "group"').groupBy('teams').agg(
                F.avg(dynamic_parameters[0]).alias(dynamic_parameters[0]),
                F.avg(dynamic_parameters[1]).alias(dynamic_parameters[1]),
                F.avg(dynamic_parameters[2]).alias(dynamic_parameters[2]),
                F.avg(dynamic_parameters[3]).alias(dynamic_parameters[3]),
                F.avg(dynamic_parameters[4]).alias(dynamic_parameters[4]),
                F.avg(dynamic_parameters[5]).alias(dynamic_parameters[5]),
            ), 'teams', 'left') \
                .fillna(-1, dynamic_parameters) \
                .withColumn('estimated_variables', estimate_avg_concurrency_udf('match_stage', 'teams', 'match_type',
                                                                                'active_frees_rate',
                                                                                'frees_watching_match_rate',
                                                                                "watch_time_per_free_per_match",
                                                                                'active_subscribers_rate',
                                                                                'subscribers_watching_match_rate',
                                                                                "watch_time_per_subscriber_per_match",
                                                                                'avg_india_active_frees_rate',
                                                                                'avg_india_frees_watching_match_rate',
                                                                                'avg_india_watch_time_per_free_per_match',
                                                                                'avg_india_active_subscribers_rate',
                                                                                'avg_india_subscribers_watching_match_rate',
                                                                                'avg_india_watch_time_per_subscriber_per_match',
                                                                                'avg_stage_1_active_frees_rate',
                                                                                'avg_stage_1_frees_watching_match_rate',
                                                                                'avg_stage_1_watch_time_per_free_per_match',
                                                                                'avg_stage_1_active_subscribers_rate',
                                                                                'avg_stage_1_subscribers_watching_match_rate',
                                                                                'avg_stage_1_watch_time_per_subscriber_per_match',
                                                                                'avg_stage_2_active_frees_rate',
                                                                                'avg_stage_2_frees_watching_match_rate',
                                                                                'avg_stage_2_watch_time_per_free_per_match',
                                                                                'avg_stage_2_active_subscribers_rate',
                                                                                'avg_stage_2_subscribers_watching_match_rate',
                                                                                'avg_stage_2_watch_time_per_subscriber_per_match',
                                                                                'avg_sf_active_frees_rate',
                                                                                'avg_sf_frees_watching_match_rate',
                                                                                'avg_sf_watch_time_per_free_per_match',
                                                                                'avg_sf_active_subscribers_rate',
                                                                                'avg_sf_subscribers_watching_match_rate',
                                                                                'avg_sf_watch_time_per_subscriber_per_match',
                                                                                'avg_final_active_frees_rate',
                                                                                'avg_final_frees_watching_match_rate',
                                                                                'avg_final_watch_time_per_free_per_match',
                                                                                'avg_final_active_subscribers_rate',
                                                                                'avg_final_subscribers_watching_match_rate',
                                                                                'avg_final_watch_time_per_subscriber_per_match')) \
                .withColumn('estimated_free_rate', F.col('estimated_variables').getItem(0)) \
                .withColumn('estimated_free_watch_rate', F.col('estimated_variables').getItem(1)) \
                .withColumn('estimated_free_watch_time', F.col('estimated_variables').getItem(2)) \
                .withColumn('estimated_sub_rate', F.col('estimated_variables').getItem(3)) \
                .withColumn('estimated_sub_watch_rate', F.col('estimated_variables').getItem(4)) \
                .withColumn('estimated_sub_watch_time', F.col('estimated_variables').getItem(5)) \
                .withColumn('estimated_total_free_watch_time', F.expr('estimated_free_num * estimated_free_watch_rate * estimated_free_watch_time')) \
                .withColumn('estimated_total_sub_watch_time', F.expr('estimated_sub_num * estimated_sub_watch_rate * estimated_sub_watch_time'))
            # print(new_test_label_df.count())
            save_data_frame(new_test_label_df.select('date', 'content_id', 'estimated_total_free_watch_time',
                                                     'estimated_total_sub_watch_time').orderBy('date', 'content_id'),
                            live_ads_inventory_forecasting_root_path + f"/total_free_and_sub_wt_prediction_by_baseline/test_tournament={test_tournament}")
            for configuration in configurations[1:2]:
                total_match_duration_in_minutes, number_of_ad_breaks, average_length_of_a_break_in_seconds = configuration
                res_df = new_test_label_df \
                    .drop('estimated_total_free_watch_time', 'estimated_total_sub_watch_time') \
                    .withColumn('real_avg_concurrency', F.expr(
                    f'(total_frees_number * real_frees_watching_match_rate * real_watch_time_per_free_per_match '
                    f'+ total_subscribers_number * real_subscribers_watching_match_rate * real_watch_time_per_subscriber_per_match)'
                    f'/{total_match_duration_in_minutes}')) \
                    .withColumn('estimated_avg_concurrency', F.expr(
                    f'(estimated_free_num * estimated_free_watch_rate * estimated_free_watch_time '
                    f'+ estimated_sub_num * estimated_sub_watch_rate * estimated_sub_watch_time)/{total_match_duration_in_minutes}')) \
                    .withColumn('estimated_inventory', F.expr(
                    f'estimated_avg_concurrency * {drop_off_rate} * ({number_of_ad_breaks * average_length_of_a_break_in_seconds} / 10.0)')) \
                    .withColumn('estimated_reach', F.expr(
                    f"(estimated_free_num * estimated_free_watch_rate / {sub_pid_did_rate}) + (estimated_sub_num * estimated_sub_watch_rate / {free_pid_did_rate})")) \
                    .withColumn('estimated_inventory', F.expr('cast(estimated_inventory as bigint)')) \
                    .withColumn('estimated_reach', F.expr('cast(estimated_reach as bigint)')) \
                    .withColumn('avg_concurrency_bias',
                                F.expr('(estimated_avg_concurrency - real_avg_concurrency) / real_avg_concurrency')) \
                    .withColumn('reach_bias', F.expr('(estimated_reach - total_did_reach) / total_did_reach')) \
                    .withColumn('inventory_bias', F.expr('(estimated_inventory - total_inventory) / total_inventory')) \
                    .withColumn('inventory_bias_abs', F.expr('abs(estimated_inventory - total_inventory)')) \
                    .withColumn('inventory_bias_abs_rate', F.expr('inventory_bias_abs / total_inventory')) \
                    .where('total_inventory > 0') \
                    .drop('tournament', 'match_stage_detail') \
                    .cache()
                # print(res_df.count())
                cols = res_df.columns
                important_cols = ["real_avg_concurrency", "estimated_avg_concurrency", "avg_concurrency_bias",
                                  "total_did_reach", 'estimated_reach', "reach_bias",
                                  "total_inventory", "estimated_inventory", "inventory_bias", 'inventory_bias_abs']
                for col in important_cols:
                    cols.remove(col)
                final_cols = cols + important_cols
                # print(final_cols)
                res_df = res_df.select(*final_cols).orderBy('date', 'content_id')
                save_data_frame(res_df,
                                live_ads_inventory_forecasting_root_path + f"/test_result_of_{test_tournament}_using_{version}")
                res_list.append(res_df
                                .withColumn('tournament', F.lit(test_tournament))
                                .withColumn('match_type', F.lit(match_type[0])))
                # save_data_frame(res_df, live_ads_inventory_forecasting_root_path+f"/test_result_of_{test_tournament}_using_{version}_2")
                res_df.select('date', 'title', 'estimated_inventory').show(200, False)
                # res_df.show(200, False)
                # print(configuration)
                # res_df \
                #     .groupBy('shortsummary') \
                #     .agg(F.sum('total_inventory').alias('total_inventory'),
                #          F.sum('estimated_inventory').alias('estimated_inventory')) \
                #     .withColumn('bias', F.expr('(estimated_inventory - total_inventory) / total_inventory')) \
                #     .show(200, False)
                # res_df \
                #     .groupBy('shortsummary') \
                #     .agg(F.sum('real_avg_concurrency').alias('real_avg_concurrency'),
                #          F.sum('estimated_avg_concurrency').alias('estimated_avg_concurrency')) \
                #     .withColumn('bias', F.expr('(estimated_avg_concurrency - real_avg_concurrency) / real_avg_concurrency')) \
                #     .show(200, False)
        # <-- for raw baseline with simple rules -->
        # <-- for optimized baseline with feature similarity -->
        # <-- for optimized baseline with predicted parameters -->
        if version in ["baseline_with_predicted_parameters"]:
            label_cols = ['frees_watching_match_rate', "watch_time_per_free_per_match",
                          'subscribers_watching_match_rate', "watch_time_per_subscriber_per_match"]
            # print(first_match_date)
            new_test_label_df = test_df \
                .withColumn('estimated_variables', F.lit(0)) \
                .join(load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/xgb_prediction{mask_tag}/{test_tournament}/{label_cols[0]}")
                .drop('sample_tag', 'real_' + label_cols[0]), ['date', 'content_id']) \
                .join(load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/xgb_prediction{mask_tag}/{test_tournament}/{label_cols[1]}").drop(
                'sample_tag', 'real_' + label_cols[1]), ['date', 'content_id']) \
                .join(load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/xgb_prediction{mask_tag}/{test_tournament}/{label_cols[2]}").drop(
                'sample_tag', 'real_' + label_cols[2]), ['date', 'content_id']) \
                .join(load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/xgb_prediction{mask_tag}/{test_tournament}/{label_cols[3]}").drop(
                'sample_tag', 'real_' + label_cols[3]), ['date', 'content_id']) \
                .cache()
            for configuration in configurations[1:2]:
                total_match_duration_in_minutes, number_of_ad_breaks, average_length_of_a_break_in_seconds = configuration
                res_df = new_test_label_df \
                    .withColumn('real_avg_concurrency', F.expr(
                    f'(total_frees_number * real_frees_watching_match_rate * real_watch_time_per_free_per_match '
                    f'+ total_subscribers_number * real_subscribers_watching_match_rate * real_watch_time_per_subscriber_per_match)'
                    f'/{total_match_duration_in_minutes}')) \
                    .withColumn('estimated_avg_concurrency', F.expr(
                    f'(estimated_free_num * estimated_free_watch_rate * estimated_free_watch_time '
                    f'+ estimated_sub_num * estimated_sub_watch_rate * estimated_sub_watch_time)/{total_match_duration_in_minutes}')) \
                    .withColumn('estimated_inventory', F.expr(
                    f'estimated_avg_concurrency * {drop_off_rate} * ({number_of_ad_breaks * average_length_of_a_break_in_seconds} / 10.0)')) \
                    .withColumn('estimated_reach', F.expr(
                    f"(estimated_free_num * estimated_free_watch_rate / {sub_pid_did_rate}) + (estimated_sub_num * estimated_sub_watch_rate / {free_pid_did_rate})")) \
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
                # print(final_cols)
                res_df = res_df.select(*final_cols).orderBy('date', 'content_id')
                save_data_frame(res_df,
                                live_ads_inventory_forecasting_root_path + f"/test_result_of_{test_tournament}_using_{version}{mask_tag}")
                # save_data_frame(res_df, live_ads_inventory_forecasting_root_path+f"/test_result_of_{test_tournament}_using_{version}_2")
                # res_df.select('date', 'title', 'estimated_inventory').show(200, False)
                # res_df.show(200, False)
                # print(configuration)
                # res_df \
                #     .groupBy('shortsummary') \
                #     .agg(F.sum('total_inventory').alias('total_inventory'),
                #          F.sum('estimated_inventory').alias('estimated_inventory')) \
                #     .withColumn('bias', F.expr('(estimated_inventory - total_inventory) / total_inventory')) \
                #     .show(200, False)
                # res_df \
                #     .groupBy('shortsummary') \
                #     .agg(F.sum('real_avg_concurrency').alias('real_avg_concurrency'),
                #          F.sum('estimated_avg_concurrency').alias('estimated_avg_concurrency')) \
                #     .withColumn('bias', F.expr('(estimated_avg_concurrency - real_avg_concurrency) / real_avg_concurrency')) \
                #     .show(200, False)
                res_list.append(res_df \
                                .withColumn('tournament', F.lit(test_tournament))
                                .withColumn('match_type', F.lit(match_type[0])))
        # <-- for optimized baseline with predicted parameters -->
    print("")
    print("")


tournament_dic = {
    "wc2023": -1,
    "wc2022": 0,
    "ac2022": 1,
    "ipl2022": 2,
    "wc2021": 3,
    "wc2019": 4,
}
tag_mapping_udf = F.udf(lambda x: tournament_dic[x], IntegerType())


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
    .withColumn('concurrency_bias', F.expr('(estimated_avg_concurrency - real_avg_concurrency) / real_avg_concurrency')) \
    .withColumn('total_error', F.expr('(estimated_inventory - total_inventory) / total_inventory')) \
    .withColumn('total_match_error', F.expr('sum_inventory_abs_error / total_inventory')) \
    .withColumn('tag', tag_mapping_udf('tournament')) \
    .orderBy('tag')\
    .drop('tag')\
    .show(100, False)


prediction_df = reduce(lambda x, y: x.union(y), [load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/test_result_of_{tournament}_using_{version}{mask_tag}")
                                 for tournament in ["wc2019", "wc2021", "ipl2022", "ac2022", "wc2022", "wc2023"]])\
    .orderBy('date', 'content_id').select('date', 'content_id', 'title', 'tournament',
                                          'estimated_inventory', 'estimated_reach',
                                          'total_frees_number', 'real_frees_watching_match_rate', 'real_watch_time_per_free_per_match',
                                          'estimated_free_num', 'estimated_free_watch_rate', 'estimated_free_watch_time',
                                          'total_subscribers_number', 'real_subscribers_watching_match_rate', 'real_watch_time_per_subscriber_per_match',
                                          'estimated_sub_num', 'estimated_sub_watch_rate', 'estimated_sub_watch_time')\
    .withColumn('free_match_AU', F.expr('estimated_free_num * estimated_free_watch_rate'))\
    .withColumn('sub_match_AU', F.expr('estimated_sub_num * estimated_sub_watch_rate'))\
    .withColumn('free_inventory', F.expr('(estimated_free_num * estimated_free_watch_rate * estimated_free_watch_time) / '
                                         '(estimated_free_num * estimated_free_watch_rate * estimated_free_watch_time + '
                                         'estimated_sub_num * estimated_sub_watch_rate * estimated_sub_watch_time) * estimated_inventory'))\
    .withColumn('sub_inventory', F.expr('(estimated_sub_num * estimated_sub_watch_rate * estimated_sub_watch_time) / '
                                         '(estimated_free_num * estimated_free_watch_rate * estimated_free_watch_time + '
                                         'estimated_sub_num * estimated_sub_watch_rate * estimated_sub_watch_time) * estimated_inventory'))\
    .cache()


reduce(lambda x, y: x.union(y), [load_labels(tournament, all_feature_df) for tournament in tournament_list])\
    .where('content_id not in ("1440000689", "1440000694", "1440000696", "1440000982", "1540019005", "1540019014", "1540019017","1540016333")')\
    .join(prediction_df, ['date', 'content_id', 'title'], 'full')\
    .orderBy('date', 'content_id')\
    .show(10000, False)

