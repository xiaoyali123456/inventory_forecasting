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
    sub_df = sub_df\
        .where(f'sub_start_time <= "{target_date}" and sub_end_time >= "{target_date}"')\
        .select('hid')\
        .distinct()\
        .join(user_meta_df, 'hid')
    return sub_df.select('dw_p_id').distinct()


def estimate_avg_concurrency(match_stage_detail, title,
                       active_frees_rate, frees_watching_match_rate, watch_time_per_free_per_match,
                       active_subscribers_rate, subscribers_watching_match_rate, watch_time_per_subscriber_per_match,
                       avg_india_active_frees_rate, avg_india_frees_watching_match_rate, avg_india_watch_time_per_free_per_match,
                       avg_india_active_subscribers_rate, avg_india_subscribers_watching_match_rate, avg_india_watch_time_per_subscriber_per_match,
                       avg_stage_1_active_frees_rate, avg_stage_1_frees_watching_match_rate, avg_stage_1_watch_time_per_free_per_match,
                       avg_stage_1_active_subscribers_rate, avg_stage_1_subscribers_watching_match_rate, avg_stage_1_watch_time_per_subscriber_per_match,
                       avg_stage_2_active_frees_rate, avg_stage_2_frees_watching_match_rate, avg_stage_2_watch_time_per_free_per_match,
                       avg_stage_2_active_subscribers_rate, avg_stage_2_subscribers_watching_match_rate, avg_stage_2_watch_time_per_subscriber_per_match,
                       avg_sf_active_frees_rate, avg_sf_frees_watching_match_rate, avg_sf_watch_time_per_free_per_match,
                       avg_sf_active_subscribers_rate, avg_sf_subscribers_watching_match_rate, avg_sf_watch_time_per_subscriber_per_match,
                       avg_final_active_frees_rate, avg_final_frees_watching_match_rate, avg_final_watch_time_per_free_per_match,
                       avg_final_active_subscribers_rate, avg_final_subscribers_watching_match_rate, avg_final_watch_time_per_subscriber_per_match):
    if match_stage_detail == "semi-final":
        free_rate = avg_sf_active_frees_rate
        free_watch_rate = avg_sf_frees_watching_match_rate
        free_watch_time = avg_sf_watch_time_per_free_per_match
        sub_rate = avg_sf_active_subscribers_rate
        sub_watch_rate = avg_sf_subscribers_watching_match_rate
        sub_watch_time = avg_sf_watch_time_per_subscriber_per_match
        match_type = 1
    elif match_stage_detail == "final":
        free_rate = avg_final_active_frees_rate
        free_watch_rate = avg_final_frees_watching_match_rate
        free_watch_time = avg_final_watch_time_per_free_per_match
        sub_rate = avg_final_active_subscribers_rate
        sub_watch_rate = avg_final_subscribers_watching_match_rate
        sub_watch_time = avg_final_watch_time_per_subscriber_per_match
        match_type = 2
    elif active_frees_rate > 0:
        free_rate = active_frees_rate
        free_watch_rate = frees_watching_match_rate
        free_watch_time = watch_time_per_free_per_match
        sub_rate = active_subscribers_rate
        sub_watch_rate = subscribers_watching_match_rate
        sub_watch_time = watch_time_per_subscriber_per_match
        match_type = 3
    elif title.find("india") > -1:
        free_rate = avg_india_active_frees_rate
        free_watch_rate = avg_india_frees_watching_match_rate
        free_watch_time = avg_india_watch_time_per_free_per_match
        sub_rate = avg_india_active_subscribers_rate
        sub_watch_rate = avg_india_subscribers_watching_match_rate
        sub_watch_time = avg_india_watch_time_per_subscriber_per_match
        match_type = 4
    elif match_stage_detail == "group-stage-1":
        free_rate = avg_stage_1_active_frees_rate
        free_watch_rate = avg_stage_1_frees_watching_match_rate
        free_watch_time = avg_stage_1_watch_time_per_free_per_match
        sub_rate = avg_stage_1_active_subscribers_rate
        sub_watch_rate = avg_stage_1_subscribers_watching_match_rate
        sub_watch_time = avg_stage_1_watch_time_per_subscriber_per_match
        match_type = 5
    else:
        free_rate = avg_stage_2_active_frees_rate
        free_watch_rate = avg_stage_2_frees_watching_match_rate
        free_watch_time = avg_stage_2_watch_time_per_free_per_match
        sub_rate = avg_stage_2_active_subscribers_rate
        sub_watch_rate = avg_stage_2_subscribers_watching_match_rate
        sub_watch_time = avg_stage_2_watch_time_per_subscriber_per_match
        match_type = 6
    if match_stage_detail.find("final") > -1 and title.find("india") > -1:
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
def calculate_similarity_between_two_matches(target_feature, feature, feature_num):
    similarity = 0.0
    if version == "baseline_with_feature_similarity":
        for i in range(feature_num):
            similarity += float(cosine_similarity([target_feature[i]], [feature[i]])[0][0])
        return similarity / feature_num
    else:
        for i in range(feature_num):
            similarity += feature_weights[i] * float(cosine_similarity([target_feature[i]], [feature[i]])[0][0])
        return similarity


def estimate_avg_concurrency_using_feature_similarity(*args):
    similarity_list = []
    for content_key in valid_feature_dic:
        similarity_list.append((content_key, calculate_similarity_between_two_matches(args, valid_feature_dic[content_key], feature_num)))
    final_similarity_list = sorted(similarity_list, key=lambda content_key: content_key[1], reverse=True)[:top_N_matches]
    # print(final_similarity_list)
    res = [0.0 for i in range(dynamic_parameter_num)]
    similarity_sum = 0.0
    for content_key in final_similarity_list:
        similarity_sum += content_key[1]
    if similarity_sum == 0.0:
        similarity_sum = 1.0
    for content_key in final_similarity_list:
        res = list(map(lambda x: x[0]+x[1], zip(res, [content_key[1]/similarity_sum * x for x in valid_parameter_dic[content_key[0]]])))
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
    return load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/baseline_features_final/all_features_hots_format")\
        .where(f'tournament="{tournament}"')


def load_labels(tournament):
    data_source = "watched_video_sampled"
    df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/final_test_dataset/{data_source}_of_{tournament}")
    # date, content_id, title, shortsummary,
    # total_inventory, total_pid_reach, total_did_reach
    return df


def first_match_data(df, col):
    return df.where('rank = 1').select(col).collect()[0][0]


def last_match_data(df, col):
    return df.where(f'rank = {df.count()}').select(col).collect()[0][0]


def days_bewteen_st_and_et(st, et):
    date1 = datetime.datetime.strptime(st, "%Y-%m-%d").date()
    date2 = datetime.datetime.strptime(et, "%Y-%m-%d").date()
    return (date2 - date1).days


def get_match_stage(tournament, rank):
    if tournament.find("wc") > -1:
        if rank == 43 or rank == 44:
            return "semi-final"
        elif rank == 45:
            return "final"
        elif rank <= 12:
            return "group-stage-1"
        else:
            return "group-stage-2"
    elif tournament.find("ac") > -1:
        if rank == 13:
            return "final"
        # elif rank <= 6:
        #     return "group-stage-1"
        else:
            return "group-stage-2"
    else:
        return ""


check_title_valid_udf = F.udf(check_title_valid, IntegerType())
get_match_stage_udf = F.udf(get_match_stage, StringType())
estimate_avg_concurrency_udf = F.udf(estimate_avg_concurrency, ArrayType(FloatType()))
estimate_avg_concurrency_using_feature_similarity_udf = F.udf(estimate_avg_concurrency_using_feature_similarity, ArrayType(FloatType()))

concurrency_root_path = "s3://hotstar-dp-datalake-processed-us-east-1-prod/hive_internal_database/concurrency.db/"
ssai_concurrency_path = f"{concurrency_root_path}/users_by_live_sports_content_by_ssai"
match_meta_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/ads_crash/match_meta"
live_ads_inventory_forecasting_root_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting"
play_out_log_input_path = "s3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test"
watchAggregatedInputPath = "s3://hotstar-dp-datalake-processed-us-east-1-prod/aggregates/watched_video_daily_aggregates_ist_v4"
viewAggregatedInputPath = "s3://hotstar-dp-datalake-processed-us-east-1-prod/aggregates/viewed_page_daily_aggregates_ist_v2"

# spark.stop()
# spark = hive_spark('statistics')
# match_df = load_hive_table(spark, "in_cms.match_update_s3")
# save_data_frame(match_df, match_meta_path)
tournament_dic = {"wc2022": "ICC Men\'s T20 World Cup 2022",
                  "ac2022": "DP World Asia Cup 2022",
                  "ipl2022": "TATA IPL 2022",
                  "wc2021": "ICC Men\'s T20 World Cup 2021",
                  "ipl2021": "VIVO IPL 2021",
                  "wc2019": "ICC CWC 2019"}

# total_match_duration_in_minutes, number_of_ad_breaks, average_length_of_a_break_in_seconds = 325.0, 55.0, 80.0
# total_match_duration_in_minutes, number_of_ad_breaks, average_length_of_a_break_in_seconds = 210.0, 85.0, 30.0
# total_match_duration_in_minutes, number_of_ad_breaks, average_length_of_a_break_in_seconds = 210.0, 45.0, 55.0
configurations = [(325.0, 55.0, 80.0), (210.0, 55.0, 80.0), (210.0, 85.0, 30.0), (210.0, 45.0, 55.0)]
drop_off_rate = 0.8
dynamic_parameters = ['active_frees_rate', 'frees_watching_match_rate', "watch_time_per_free_per_match",
                      'active_subscribers_rate', 'subscribers_watching_match_rate', "watch_time_per_subscriber_per_match"]
dynamic_parameter_num = len(dynamic_parameters)
one_hot_cols = ['if_weekend', 'match_time', 'if_holiday', 'venue', 'if_contain_india_team', 'match_type', 'tournament_name', 'hostar_influence', 'match_stage', 'gender_type']
multi_hot_cols = ['teams', 'teams_tier']
additional_cols = ["languages", "platforms"]
invalid_cols = ['gender_type', 'teams_tier']
feature_cols = [col+"_hot_vector" for col in one_hot_cols[:-1]+multi_hot_cols[:-1]+additional_cols]
feature_num = len(feature_cols)
top_N_matches = 5
# test_tournament = "wc2022"
test_tournament = "ac2022"
# version = "baseline"
# version = "baseline_with_feature_similarity"
version = "baseline_with_weighted_feature_similarity"
sub_version = 1
feature_weights_list = [[0.048771217289997115, 0.0157881372804255, 0.0026336662551354937, 0.0, 0.0, 0.0,
                     0.0, 0.0, 0.010433410345705944, 0.6480424955288829, 0.2663948955869045, 0.00793617771294856],
                   [0.007757412284001172, 0.09018759720574084, 0.008966386666609202, 0.0, 0.0, 0.0,
                     0.0, 0.0, 0.29620880638005725, 0.7166922309997653, 0.0, 0.0],
                   [0.0, 0.06518549439531932, 0.0, 0.0, 0.0, 0.0,
                     0.0, 0.0, 0.0, 0.6197393111586827, 0.34560371694760716, 0.0],
                   [0.0, 0.26135379474690607, 0.0003931491521634271, 0.0, 0.0,
                     0.0, 0.0, 0.0, 0.15159977769603067, 0.6049552042585151, 0.0, 0.0]
]
for i in range(4):
    l = [(feature_cols[j], feature_weights_list[i][j]) for j in range(feature_num)]
    print(sorted(l, key=lambda x: x[1], reverse=True))

feature_weights = feature_weights_list[sub_version]
sub_pid_did_rate = 0.94
free_pid_did_rate = 1.02

# calculation for wc 2022
# total_free_number: first-match = first match of wc 2021, rest matches are increasing by rate in match level of wc 2021
# total_sub_number: first-match = last match of ipl 2022 + normal-increasing-rate-in-date-level * days-between-ipl-2022-and-wc-2022, rest matches are increasing by rate in match level of wc 2021
# wt-related-parameters: select most similar matches of wc 2021, and use the avg value of the parameters in those matches
#     order: knockout, contains india + 10%
# total_match_duration = X min, number_of_ad_breaks and average_length_of_a_break_in_seconds used the same as wc 2021

feature_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/baseline_features_final/all_features_hots_format")\
    .where('date != "2022-08-24"')\
    .cache()
wc2021_feature_df = feature_df.where(f"tournament='wc2021'").cache()
ipl2022_feature_df = feature_df.where(f"tournament='ipl2022'").cache()
test_feature_df = feature_df\
    .where(f"tournament='{test_tournament}'")\
    .selectExpr('content_id', 'rank', 'teams', 'tournament', *feature_cols, 'total_frees_number', 'active_frees_rate as real_active_frees_rate',
                'frees_watching_match_rate as real_frees_watching_match_rate', 'watch_time_per_free_per_match as real_watch_time_per_free_per_match',
                'total_subscribers_number', 'active_subscribers_rate as real_active_subscribers_rate',
                'subscribers_watching_match_rate as real_subscribers_watching_match_rate', 'watch_time_per_subscriber_per_match as real_watch_time_per_subscriber_per_match')\
    .cache()
test_label_df = load_labels(f"{test_tournament}")\
    .join(test_feature_df, 'content_id')\
    .withColumn('rank', F.expr('row_number() over (partition by tournament order by date)'))\
    .withColumn('match_stage_detail', get_match_stage_udf('tournament', 'rank'))\
    .cache()

# test_label_df.select('title', 'content_id', 'date', 'match_stage_detail').orderBy('content_id').show(50, False)

first_match_free_num = first_match_data(wc2021_feature_df, 'total_frees_number')
free_num_increasing_rate = (last_match_data(wc2021_feature_df, 'total_frees_number') - first_match_free_num) / (wc2021_feature_df.count() - 1)
first_match_sub_num = last_match_data(ipl2022_feature_df, 'total_subscribers_number') + \
                      ((first_match_data(ipl2022_feature_df, 'total_subscribers_number') - last_match_data(wc2021_feature_df, 'total_subscribers_number')) /
                       days_bewteen_st_and_et(last_match_data(wc2021_feature_df, 'date'), first_match_data(ipl2022_feature_df, 'date')))\
                      * days_bewteen_st_and_et(last_match_data(ipl2022_feature_df, 'date'), first_match_data(test_label_df, 'date'))
sub_num_increasing_rate = (last_match_data(wc2021_feature_df, 'total_subscribers_number') - first_match_data(wc2021_feature_df, 'total_subscribers_number')) / (wc2021_feature_df.count() - 1)
first_match_date = first_match_data(test_label_df, 'date')
print(first_match_date)
test_label_df = test_label_df\
    .withColumn('datediff', F.datediff(F.col('date'), F.lit(first_match_date)))\
    .cache()

# <-- for raw baseline with simple rules -->
if version == "baseline":
    india_match_df = wc2021_feature_df\
        .where('if_contain_india_team = 1 and rank < 43')\
        .groupBy('shortsummary')\
        .agg(F.avg('active_frees_rate').alias('avg_india_active_frees_rate'),
             F.avg('frees_watching_match_rate').alias('avg_india_frees_watching_match_rate'),
             F.avg('watch_time_per_free_per_match').alias('avg_india_watch_time_per_free_per_match'),
             F.avg('active_subscribers_rate').alias('avg_india_active_subscribers_rate'),
             F.avg('subscribers_watching_match_rate').alias('avg_india_subscribers_watching_match_rate'),
             F.avg('watch_time_per_subscriber_per_match').alias('avg_india_watch_time_per_subscriber_per_match'))\
        .cache()
    # print(india_match_df.columns)
    stage_1_match_df = wc2021_feature_df\
        .where('if_contain_india_team = 0 and rank <= 12')\
        .groupBy('shortsummary')\
        .agg(F.avg('active_frees_rate').alias('avg_stage_1_active_frees_rate'),
             F.avg('frees_watching_match_rate').alias('avg_stage_1_frees_watching_match_rate'),
             F.avg('watch_time_per_free_per_match').alias('avg_stage_1_watch_time_per_free_per_match'),
             F.avg('active_subscribers_rate').alias('avg_stage_1_active_subscribers_rate'),
             F.avg('subscribers_watching_match_rate').alias('avg_stage_1_subscribers_watching_match_rate'),
             F.avg('watch_time_per_subscriber_per_match').alias('avg_stage_1_watch_time_per_subscriber_per_match'))\
        .cache()
    # print(stage_1_match_df.columns)
    stage_2_match_df = wc2021_feature_df\
        .where('if_contain_india_team = 0 and rank >= 13 and rank <= 42')\
        .groupBy('shortsummary')\
        .agg(F.avg('active_frees_rate').alias('avg_stage_2_active_frees_rate'),
             F.avg('frees_watching_match_rate').alias('avg_stage_2_frees_watching_match_rate'),
             F.avg('watch_time_per_free_per_match').alias('avg_stage_2_watch_time_per_free_per_match'),
             F.avg('active_subscribers_rate').alias('avg_stage_2_active_subscribers_rate'),
             F.avg('subscribers_watching_match_rate').alias('avg_stage_2_subscribers_watching_match_rate'),
             F.avg('watch_time_per_subscriber_per_match').alias('avg_stage_2_watch_time_per_subscriber_per_match'))\
        .cache()
    # print(stage_2_match_df.columns)
    sf_match_df = wc2021_feature_df\
        .where('rank in (43, 44)')\
        .groupBy('shortsummary')\
        .agg(F.avg('active_frees_rate').alias('avg_sf_active_frees_rate'),
             F.avg('frees_watching_match_rate').alias('avg_sf_frees_watching_match_rate'),
             F.avg('watch_time_per_free_per_match').alias('avg_sf_watch_time_per_free_per_match'),
             F.avg('active_subscribers_rate').alias('avg_sf_active_subscribers_rate'),
             F.avg('subscribers_watching_match_rate').alias('avg_sf_subscribers_watching_match_rate'),
             F.avg('watch_time_per_subscriber_per_match').alias('avg_sf_watch_time_per_subscriber_per_match'))\
        .cache()
    # print(sf_match_df.columns)
    final_match_df = wc2021_feature_df\
        .where('rank in (45)')\
        .groupBy('shortsummary')\
        .agg(F.avg('active_frees_rate').alias('avg_final_active_frees_rate'),
             F.avg('frees_watching_match_rate').alias('avg_final_frees_watching_match_rate'),
             F.avg('watch_time_per_free_per_match').alias('avg_final_watch_time_per_free_per_match'),
             F.avg('active_subscribers_rate').alias('avg_final_active_subscribers_rate'),
             F.avg('subscribers_watching_match_rate').alias('avg_final_subscribers_watching_match_rate'),
             F.avg('watch_time_per_subscriber_per_match').alias('avg_final_watch_time_per_subscriber_per_match'))\
        .cache()
    # print(final_match_df.columns)
    # avg_concurrency = ((float(total_free_num) * free_rate * free_watch_rate * free_watch_time)
    #                        + (float(total_sub_num) * sub_rate * sub_watch_rate * sub_watch_time))/total_match_duration_in_minutes
    # res = [free_rate, free_watch_rate, free_watch_time, sub_rate, sub_watch_rate, sub_watch_time]
    test_label_df = test_label_df \
            .withColumn('estimated_free_num', F.expr(f"{first_match_free_num} + datediff * {free_num_increasing_rate}"))\
            .withColumn('estimated_sub_num', F.expr(f"{first_match_sub_num} + datediff * {sub_num_increasing_rate}"))\
            .crossJoin(F.broadcast(india_match_df.drop('shortsummary')))\
            .crossJoin(F.broadcast(stage_1_match_df.drop('shortsummary')))\
            .crossJoin(F.broadcast(stage_2_match_df.drop('shortsummary')))\
            .crossJoin(F.broadcast(sf_match_df.drop('shortsummary')))\
            .crossJoin(F.broadcast(final_match_df.drop('shortsummary')))\
            .join(wc2021_feature_df.where('rank <= 42').select('teams', *dynamic_parameters), 'teams', 'left')\
            .fillna(-1, dynamic_parameters)\
            .withColumn('estimated_variables', estimate_avg_concurrency_udf('match_stage_detail', 'title',
                                                                      'active_frees_rate', 'frees_watching_match_rate', "watch_time_per_free_per_match",
                                                                      'active_subscribers_rate', 'subscribers_watching_match_rate', "watch_time_per_subscriber_per_match",
                                                                      'avg_india_active_frees_rate', 'avg_india_frees_watching_match_rate', 'avg_india_watch_time_per_free_per_match',
                                                                      'avg_india_active_subscribers_rate', 'avg_india_subscribers_watching_match_rate', 'avg_india_watch_time_per_subscriber_per_match',
                                                                      'avg_stage_1_active_frees_rate', 'avg_stage_1_frees_watching_match_rate', 'avg_stage_1_watch_time_per_free_per_match',
                                                                      'avg_stage_1_active_subscribers_rate', 'avg_stage_1_subscribers_watching_match_rate', 'avg_stage_1_watch_time_per_subscriber_per_match',
                                                                      'avg_stage_2_active_frees_rate', 'avg_stage_2_frees_watching_match_rate', 'avg_stage_2_watch_time_per_free_per_match',
                                                                      'avg_stage_2_active_subscribers_rate', 'avg_stage_2_subscribers_watching_match_rate', 'avg_stage_2_watch_time_per_subscriber_per_match',
                                                                      'avg_sf_active_frees_rate', 'avg_sf_frees_watching_match_rate', 'avg_sf_watch_time_per_free_per_match',
                                                                      'avg_sf_active_subscribers_rate', 'avg_sf_subscribers_watching_match_rate', 'avg_sf_watch_time_per_subscriber_per_match',
                                                                      'avg_final_active_frees_rate', 'avg_final_frees_watching_match_rate', 'avg_final_watch_time_per_free_per_match',
                                                                      'avg_final_active_subscribers_rate', 'avg_final_subscribers_watching_match_rate', 'avg_final_watch_time_per_subscriber_per_match'))\
            .withColumn('estimated_free_rate', F.col('estimated_variables').getItem(0))\
            .withColumn('estimated_free_watch_rate', F.col('estimated_variables').getItem(1))\
            .withColumn('estimated_free_watch_time', F.col('estimated_variables').getItem(2))\
            .withColumn('estimated_sub_rate', F.col('estimated_variables').getItem(3))\
            .withColumn('estimated_sub_watch_rate', F.col('estimated_variables').getItem(4))\
            .withColumn('estimated_sub_watch_time', F.col('estimated_variables').getItem(5))
    for configuration in configurations:
        total_match_duration_in_minutes, number_of_ad_breaks, average_length_of_a_break_in_seconds = configuration
        res_df = test_label_df \
            .withColumn('real_avg_concurrency', F.expr(f'(total_frees_number * real_active_frees_rate * real_frees_watching_match_rate * real_watch_time_per_free_per_match '
                f'+ total_subscribers_number * real_active_subscribers_rate * real_subscribers_watching_match_rate * real_watch_time_per_subscriber_per_match)'
                f'/{total_match_duration_in_minutes}')) \
            .withColumn('estimated_avg_concurrency', F.expr(f'(estimated_free_num * estimated_free_rate * estimated_free_watch_rate * estimated_free_watch_time '
                                                            f'+ estimated_sub_num * estimated_sub_rate * estimated_sub_watch_rate * estimated_sub_watch_time)/{total_match_duration_in_minutes}'))\
            .withColumn('estimated_inventory', F.expr(f'estimated_avg_concurrency * {drop_off_rate} * ({number_of_ad_breaks * average_length_of_a_break_in_seconds} / 10.0)')) \
            .withColumn('estimated_reach', F.expr(f"(estimated_free_num * estimated_free_rate * estimated_free_watch_rate / {sub_pid_did_rate}) + (estimated_sub_num * estimated_sub_rate * estimated_sub_watch_rate / {free_pid_did_rate})")) \
        .withColumn('estimated_inventory', F.expr('cast(estimated_inventory as bigint)'))\
            .withColumn('estimated_reach', F.expr('cast(estimated_reach as bigint)'))\
            .withColumn('avg_concurrency_bias', F.expr('(estimated_avg_concurrency - real_avg_concurrency) / real_avg_concurrency'))\
            .withColumn('reach_bias', F.expr('(estimated_reach - total_did_reach) / total_did_reach')) \
            .withColumn('inventory_bias', F.expr('(estimated_inventory - total_inventory) / total_inventory')) \
            .withColumn('inventory_bias_abs', F.expr('abs(estimated_inventory - total_inventory)'))\
            .where('total_inventory > 0') \
            .drop('tournament', 'match_stage_detail', 'datediff') \
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
        save_data_frame(res_df,
                        live_ads_inventory_forecasting_root_path + f"/test_result_of_{test_tournament}_using_{version}_{sub_version}")
        # save_data_frame(res_df, live_ads_inventory_forecasting_root_path+f"/test_result_of_{test_tournament}_using_{version}_2")
        # res_df.show(200, False)
        res_df \
            .groupBy('shortsummary') \
            .agg(F.sum('total_inventory').alias('total_inventory'),
                 F.sum('estimated_inventory').alias('estimated_inventory')) \
            .withColumn('bias', F.expr('(estimated_inventory - total_inventory) / total_inventory')) \
            .show(200, False)
        res_df \
            .groupBy('shortsummary') \
            .agg(F.sum('real_avg_concurrency').alias('real_avg_concurrency'),
                 F.sum('estimated_avg_concurrency').alias('estimated_avg_concurrency')) \
            .withColumn('bias', F.expr('(estimated_avg_concurrency - real_avg_concurrency) / real_avg_concurrency')) \
            .show(200, False)

# <-- for raw baseline with simple rules -->


# <-- for optimized baseline with feature similarity -->
if version in ["baseline_with_feature_similarity", 'baseline_with_weighted_feature_similarity']:
    print(first_match_date)
    # feature_filter = f"date < '{first_match_date}' and tournament != 'wc2019'"
    feature_filter = f"tournament != '{test_tournament}' and tournament != 'wc2019'"
    valid_feature_df = feature_df\
        .where(feature_filter)\
        .select('tournament', 'title', 'content_id', *feature_cols)\
        .collect()
    valid_feature_dic = {}
    for row in valid_feature_df:
        key = row[0] + ": " + row[1] + f" ({row[2]})"
        value = row[3:]
        valid_feature_dic[key] = value
    print(valid_feature_dic[key])
    valid_parameter_df = feature_df\
        .where(feature_filter)\
        .select('tournament', 'title', 'content_id', *dynamic_parameters)\
        .collect()
    valid_parameter_dic = {}
    for row in valid_parameter_df:
        key = row[0] + ": " + row[1] + f" ({row[2]})"
        value = row[3:]
        valid_parameter_dic[key] = list(value)
    print(valid_parameter_dic[key])
    test_label_df = test_label_df \
        .withColumn('estimated_free_num', F.expr(f"{first_match_free_num} + datediff * {free_num_increasing_rate}"))\
        .withColumn('estimated_sub_num', F.expr(f"{first_match_sub_num} + datediff * {sub_num_increasing_rate}"))\
        .withColumn('estimated_variables', estimate_avg_concurrency_using_feature_similarity_udf(*feature_cols))\
        .withColumn('estimated_free_rate', F.col('estimated_variables').getItem(0))\
        .withColumn('estimated_free_watch_rate', F.col('estimated_variables').getItem(1))\
        .withColumn('estimated_free_watch_time', F.col('estimated_variables').getItem(2))\
        .withColumn('estimated_sub_rate', F.col('estimated_variables').getItem(3))\
        .withColumn('estimated_sub_watch_rate', F.col('estimated_variables').getItem(4))\
        .withColumn('estimated_sub_watch_time', F.col('estimated_variables').getItem(5))\
        .cache()
    for configuration in configurations:
        total_match_duration_in_minutes, number_of_ad_breaks, average_length_of_a_break_in_seconds = configuration
        res_df = test_label_df \
            .withColumn('real_avg_concurrency', F.expr(f'(total_frees_number * real_active_frees_rate * real_frees_watching_match_rate * real_watch_time_per_free_per_match '
                f'+ total_subscribers_number * real_active_subscribers_rate * real_subscribers_watching_match_rate * real_watch_time_per_subscriber_per_match)'
                f'/{total_match_duration_in_minutes}')) \
            .withColumn('estimated_avg_concurrency', F.expr(f'(estimated_free_num * estimated_free_rate * estimated_free_watch_rate * estimated_free_watch_time '
                                                            f'+ estimated_sub_num * estimated_sub_rate * estimated_sub_watch_rate * estimated_sub_watch_time)/{total_match_duration_in_minutes}'))\
            .withColumn('estimated_inventory', F.expr(f'estimated_avg_concurrency * {drop_off_rate} * ({number_of_ad_breaks * average_length_of_a_break_in_seconds} / 10.0)'))\
            .withColumn('estimated_reach', F.expr(f"(estimated_free_num * estimated_free_rate * estimated_free_watch_rate / {sub_pid_did_rate}) + (estimated_sub_num * estimated_sub_rate * estimated_sub_watch_rate / {free_pid_did_rate})"))\
            .withColumn('estimated_inventory', F.expr('cast(estimated_inventory as bigint)'))\
            .withColumn('estimated_reach', F.expr('cast(estimated_reach as bigint)'))\
            .withColumn('avg_concurrency_bias', F.expr('(estimated_avg_concurrency - real_avg_concurrency) / real_avg_concurrency'))\
            .withColumn('reach_bias', F.expr('(estimated_reach - total_did_reach) / total_did_reach'))\
            .withColumn('inventory_bias', F.expr('(estimated_inventory - total_inventory) / total_inventory'))\
            .withColumn('inventory_bias_abs', F.expr('abs(estimated_inventory - total_inventory)'))\
            .where('total_inventory > 0')\
            .drop('teams', 'datediff')\
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
        save_data_frame(res_df,
                        live_ads_inventory_forecasting_root_path + f"/test_result_of_{test_tournament}_using_{version}_{sub_version}")
        # save_data_frame(res_df, live_ads_inventory_forecasting_root_path+f"/test_result_of_{test_tournament}_using_{version}_2")
        # res_df.show(200, False)
        print(configuration)
        res_df \
            .groupBy('shortsummary') \
            .agg(F.sum('total_inventory').alias('total_inventory'),
                 F.sum('estimated_inventory').alias('estimated_inventory')) \
            .withColumn('bias', F.expr('(estimated_inventory - total_inventory) / total_inventory')) \
            .show(200, False)
        res_df \
            .groupBy('shortsummary') \
            .agg(F.sum('real_avg_concurrency').alias('real_avg_concurrency'),
                 F.sum('estimated_avg_concurrency').alias('estimated_avg_concurrency')) \
            .withColumn('bias', F.expr('(estimated_avg_concurrency - real_avg_concurrency) / real_avg_concurrency')) \
            .show(200, False)

# <-- for optimized baseline with feature similarity -->



# cols = ['estimated_free_rate', 'estimated_free_watch_rate', 'estimated_free_watch_time',
#         'estimated_sub_rate', 'estimated_sub_watch_rate', 'estimated_sub_watch_time',
#         'estimated_free_num', 'estimated_sub_num', 'estimated_avg_concurrency'
#         ]
# df1 = load_data_frame(spark,
#                       live_ads_inventory_forecasting_root_path+f"/test_result_of_{test_tournament}_using_{version}_1").cache()
# df2 = load_data_frame(spark,
#                       live_ads_inventory_forecasting_root_path+f"/test_result_of_{test_tournament}_using_{version}_2").cache()
#
# df1.orderBy('date', 'content_id').select('content_id', *cols).show(200, False)
# df2.orderBy('date', 'content_id').select('content_id', *cols).show(200, False)
#
# df1.groupBy('shortsummary')\
#     .agg(F.sum('real_avg_concurrency').alias('real_avg_concurrency'),
#          F.sum('estimated_avg_concurrency').alias('estimated_avg_concurrency'))\
#     .withColumn('bias', F.expr('(estimated_avg_concurrency - real_avg_concurrency) / real_avg_concurrency'))\
#     .show(200, False)
#
# df2.groupBy('shortsummary')\
#     .agg(F.sum('real_avg_concurrency').alias('real_avg_concurrency'),
#          F.sum('estimated_avg_concurrency').alias('estimated_avg_concurrency'))\
#     .withColumn('bias', F.expr('(estimated_avg_concurrency - real_avg_concurrency) / real_avg_concurrency'))\
#     .show(200, False)

