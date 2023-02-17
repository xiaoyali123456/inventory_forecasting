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
import numpy as np
import pandas as pd

from sklearn.datasets import load_boston
from sklearn.model_selection import train_test_split
from sklearn.inspection import permutation_importance
from sklearn import metrics

from xgboost import XGBRegressor, XGBClassifier

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


def load_labels(tournament):
    if tournament == "wc2022":
        data_source = "watched_video"
    else:
        data_source = "watched_video_sampled"
    df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/final_test_dataset/{data_source}_of_{tournament}")
    # date, content_id, title, shortsummary,
    # total_inventory, total_pid_reach, total_did_reach
    return df


def load_dataset(tournaments, feature_df, sorting=False):
    tournaments_str = ",".join([f'"{tournament}"' for tournament in tournaments])
    print(tournaments_str)
    new_feature_df = feature_df \
        .where(f'date != "2022-08-24" and tournament in ({tournaments_str})') \
        .cache()
    # print(new_feature_df.count())
    # label_df = reduce(lambda x, y: x.union(y), [load_labels(tournament)
    #                   .select('content_id', 'total_inventory') for tournament in tournaments])
    # print(label_df.count())
    # df = new_feature_df \
    #     .join(label_df, 'content_id') \
    #     .toPandas()
    if sorting:
        df = new_feature_df\
            .orderBy('date', 'content_id') \
            .toPandas()
    else:
        df = new_feature_df \
            .toPandas()
    return df


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

# total_match_duration_in_minutes, number_of_ad_breaks, average_length_of_a_break_in_seconds = 240.0, 50.0, 70.0
total_match_duration_in_minutes, number_of_ad_breaks, average_length_of_a_break_in_seconds = 210.0, 85.0, 30.0
drop_off_rate = 0.8
dynamic_parameters = ['active_frees_rate', 'frees_watching_match_rate', "watch_time_per_free_per_match",
                      'active_subscribers_rate', 'subscribers_watching_match_rate', "watch_time_per_subscriber_per_match"]
dynamic_parameter_num = len(dynamic_parameters)
one_hot_cols = ['if_weekend', 'match_time', 'if_holiday', 'venue', 'if_contain_india_team', 'match_type', 'tournament_name', 'hostar_influence', 'match_stage', 'gender_type']
multi_hot_cols = ['teams', 'teams_tier']
additional_cols = ["languages", "platforms"]
invalid_cols = ['gender_type', 'teams_tier']
feature_cols = [col+"_hot_vector" for col in one_hot_cols[:-1]+multi_hot_cols[:-1]+additional_cols]
feature_num_cols = [col.replace("_hot_vector", "_hots_num") for col in feature_cols]
feature_num = len(feature_cols)
# version = "baseline"
version = "baseline_with_feature_similarity"

sub_pid_did_rate = 0.94
free_pid_did_rate = 1.02


feature_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/baseline_features_final/all_features_hots_format")\
    .cache()
label_cols = ['active_frees_rate', 'frees_watching_match_rate',
              'active_subscribers_rate', 'subscribers_watching_match_rate']
label = "total_inventory"
train_0_df = load_dataset(["wc2021", "ipl2022"], feature_df)
train_1_df = load_dataset(["wc2021", "ipl2022", "wc2022"], feature_df)
train_2_df = load_dataset(["wc2021", "ipl2022", "ac2022"], feature_df)
train_df_list = [train_0_df, train_1_df, train_2_df]
test_0_df = load_dataset(["wc2019", "ac2022", "wc2022"], feature_df, sorting=True)
# test_0_df = load_dataset(["ac2022", "wc2022"], feature_df, sorting=True)
test_1_df = load_dataset(["ac2022"], feature_df, sorting=True)
test_2_df = load_dataset(["wc2022"], feature_df, sorting=True)
test_df_list = [test_0_df, test_1_df, test_2_df]
# test_df1 = load_dataset(["wc2022"], feature_df)
# test_df2 = load_dataset(["ac2022"], feature_df)
feature_num_col_list = feature_df.select(*feature_num_cols).distinct().collect()
res_dic = {}
# for grid search

# dataset_selection = 0
# dataset_selection = 1
dataset_selection = 2
train_df = train_df_list[dataset_selection]
test_df = test_df_list[dataset_selection]

multi_col_df = []
for i in range(len(feature_cols)):
    index = [feature_cols[i]+str(j) for j in range(feature_num_col_list[0][i])]
    multi_col_df.append(train_df[feature_cols[i]].apply(pd.Series, index=index))

X_train = pd.concat(multi_col_df, axis=1)
multi_col_df = []
for i in range(len(feature_cols)):
    index = [feature_cols[i] + str(j) for j in range(feature_num_col_list[0][i])]
    multi_col_df.append(test_df[feature_cols[i]].apply(pd.Series, index=index))

X_test = pd.concat(multi_col_df, axis=1)
for label in label_cols[3:]:
    print("")
    print(label)
    res_dic[label] = []
    # for n_estimators in range(10, 100, 1000):
    for n_estimators in range(1, 100, 2):
        # print(n_estimators)
        for learning_rate in [0.01, 0.03, 0.06, 0.1, 0.2]:
            # for learning_rate in [0.1]:
            for max_depth in range(2, 10):
                # print(f"config: n_estimators={n_estimators}, max_depth={max_depth}")
                res_list = []
                y_train = train_df[label]
                # X_train, _, y_train, _ = train_test_split(X, y, test_size=1, random_state=12)
                # print(X_train.shape)
                xgb = XGBRegressor(n_estimators=n_estimators, learning_rate=learning_rate,
                                   max_depth=max_depth)
                # xgb = XGBClassifier(n_estimators=n_estimators, learning_rate=learning_rate,
                #                     max_depth=max_depth, objective='reg:pseudohubererror')
                xgb.fit(X_train, y_train)
                y_pred = xgb.predict(X_train)
                y_mean = y_train.mean()
                error = metrics.mean_absolute_error(y_train, y_pred)
                train_error = error/y_mean
                # res_list.append(f"train error: {error}/{y_mean}={error/y_mean}")
                res_list.append(f"train error: {error/y_mean}")
                # print(pd.concat([y_train, pd.DataFrame(y_pred)], axis=1))
                # print(f"train error: {error/y_mean}")
                # multi_col_df = []
                # for i in range(len(feature_cols)):
                #     index = [feature_cols[i]+str(j) for j in range(feature_num_col_list[0][i])]
                #     multi_col_df.append(test_df1[feature_cols[i]].apply(pd.Series, index=index))
                # X_test = pd.concat(multi_col_df, axis=1)
                # y_test = test_df1[label]
                # y_pred = xgb.predict(X_test)
                # y_mean = y_test.mean()
                # error = metrics.mean_absolute_error(y_test, y_pred)
                # # res_list.append(f"test error on wc2022: {error}/{y_mean}={error/y_mean}")
                # res_list.append(f"test error on wc2022: {error/y_mean}")
                # multi_col_df = []
                # for i in range(len(feature_cols)):
                #     index = [feature_cols[i]+str(j) for j in range(feature_num_col_list[0][i])]
                #     multi_col_df.append(test_df2[feature_cols[i]].apply(pd.Series, index=index))
                # X_test = pd.concat(multi_col_df, axis=1)
                # y_test = test_df2[label]
                # y_pred = xgb.predict(X_test)
                # y_mean = y_test.mean()
                # error = metrics.mean_absolute_error(y_test, y_pred)
                # # res_list.append(f"test error on ac2022: {error}/{y_mean}={error/y_mean}")
                # res_list.append(f"test error on ac2022: {error/y_mean}")
                y_test = test_df[label]
                y_pred = xgb.predict(X_test)
                y_mean = y_test.mean()
                error = metrics.mean_absolute_error(y_test, y_pred)
                # res_list.append(f"test error on ac2022: {error}/{y_mean}={error/y_mean}")
                res_list.append(f"test error: {error / y_mean}")
                # print(f"test error: {error / y_mean}")
                # print(pd.concat([y_test, pd.DataFrame(y_pred)], axis=1))
                res_dic[label].append((n_estimators, learning_rate, max_depth, error / y_mean, train_error))
                # for res in res_list:
                #     print(res)
                # print(xgb.feature_importances_)
    print(sorted(res_dic[label], key=lambda x: x[3])[:10])


# ac2022 2nd label
# [(71, 0.2, 2, 0.1744219509105058, 0.14441325795120177), (73, 0.2, 2, 0.17493892156730864, 0.14377572988649265), (61, 0.2, 2, 0.17510066320749498, 0.14820057348019333), (79, 0.2, 2, 0.1751477277044645, 0.14197657592042742), (67, 0.2, 2, 0.17523450323149903, 0.1456375658173264), (69, 0.2, 2, 0.1753412035369906, 0.14499314962299661), (77, 0.2, 2, 0.1754953146565743, 0.14240018115818628), (59, 0.2, 2, 0.17578017203386634, 0.14934722391809352), (57, 0.2, 2, 0.1759826982871979, 0.15039821296667957), (89, 0.1, 7, 0.1759862819261414, 0.0463439340818622)]

# wc2022 4th label
# [(17, 0.2, 7, 0.23141779309566848, 0.02800675945542711), (19, 0.2, 7, 0.2331088708122735, 0.024949755942655437), (39, 0.2, 7, 0.235064613501883, 0.011370194825261984), (59, 0.2, 7, 0.23510679503460635, 0.006410557675021045), (57, 0.2, 7, 0.2352057551315656, 0.006705639439372581), (55, 0.2, 7, 0.23522836963729463, 0.006933718935476563), (41, 0.2, 7, 0.2352381689918476, 0.010683505559170877), (37, 0.2, 7, 0.23524215360980602, 0.012005148062166253), (61, 0.2, 7, 0.2352544534248381, 0.006154385962629916), (37, 0.1, 7, 0.23528835291952607, 0.027486803847505265)]
# [(47, 0.2, 2, 0.08824819868320444, 0.0689003856606089), (49, 0.2, 2, 0.08854661168400506, 0.06828063636639296), (45, 0.2, 2, 0.08874398001790479, 0.06984894848493711), (39, 0.2, 2, 0.08904978560545698, 0.07197783192676857), (53, 0.2, 2, 0.08921932096218747, 0.06721576218227954), (51, 0.2, 2, 0.08937948983794519, 0.06783282665045798), (41, 0.2, 2, 0.08939781086271172, 0.07151302226903657), (43, 0.2, 2, 0.08943876592770944, 0.07066404596178236), (97, 0.2, 2, 0.08963672478782221, 0.060448245466384984), (63, 0.2, 2, 0.08965281073231018, 0.0652213653123213)]
best_parameters = [(80, 0.06, 7), (90, 0.1, 2),
                   (90, 0.1, 2), (90, 0.2, 2)]
# best_parameters = [(80, 0.1, 5), (90, 0.1, 5),
#                    (90, 0.1, 5), (90, 0.1, 5)]
for idx in range(len(label_cols)):
    label = label_cols[idx]
    print("|"+label)
    n_estimators, learning_rate, max_depth = best_parameters[idx]
    multi_col_df = []
    for i in range(len(feature_cols)):
        index = [feature_cols[i] + str(j) for j in range(feature_num_col_list[0][i])]
        multi_col_df.append(train_df[feature_cols[i]].apply(pd.Series, index=index))
    X_train = pd.concat(multi_col_df, axis=1)
    y_train = train_df[label]
    xgb = XGBRegressor(n_estimators=n_estimators, learning_rate=learning_rate,
                       max_depth=max_depth)
    # xgb = XGBRegressor(n_estimators=n_estimators, learning_rate=learning_rate,
                       # max_depth=max_depth, reg_alpha=0, reg_lambda=0)
    # xgb = XGBClassifier(n_estimators=n_estimators, learning_rate=learning_rate,
    #                     max_depth=max_depth, reg_alpha=0, reg_lambda=1)
    xgb.fit(X_train, y_train)
    y_pred = xgb.predict(X_train)
    y_mean = y_train.mean()
    error = metrics.mean_absolute_error(y_train, y_pred)
    # res_list.append(f"train error: {error}/{y_mean}={error/y_mean}")
    print(f"|train error: {error / y_mean}")
    # spark.createDataFrame(pd.concat([y_train, pd.DataFrame(y_pred)], axis=1), ['label', 'prediction']).show(1000)
    multi_col_df = []
    for i in range(len(feature_cols)):
        index = [feature_cols[i] + str(j) for j in range(feature_num_col_list[0][i])]
        multi_col_df.append(test_df[feature_cols[i]].apply(pd.Series, index=index))
    X_test = pd.concat(multi_col_df, axis=1)
    y_test = test_df[label]
    y_pred = xgb.predict(X_test)
    y_mean = y_test.mean()
    error = metrics.mean_absolute_error(y_test, y_pred)
    # res_list.append(f"test error on ac2022: {error}/{y_mean}={error/y_mean}")
    print(f"|test error: {error / y_mean}")
    # spark.createDataFrame(pd.concat([y_test, pd.DataFrame(y_pred)], axis=1), ['label', 'prediction']).show(1000)
    print("")


best_parameters = [(80, 0.06, 7), (90, 0.1, 2), (90, 0.1, 2), (90, 0.2, 2)]
for idx in range(len(label_cols)):
    label = label_cols[idx]
    print(label)
    n_estimators, learning_rate, max_depth = best_parameters[idx]
    multi_col_df = []
    for i in range(len(feature_cols)):
        index = [feature_cols[i] + str(j) for j in range(feature_num_col_list[0][i])]
        multi_col_df.append(train_df[feature_cols[i]].apply(pd.Series, index=index))
    X_train = pd.concat(multi_col_df, axis=1)
    y_train = train_df[label]
    # X_train, _, y_train, _ = train_test_split(X, y, test_size=1, random_state=12)
    # print(X_train.shape)
    xgb = XGBRegressor(n_estimators=n_estimators, learning_rate=learning_rate,
                       max_depth=max_depth, objective='binary:logistic')
    xgb.fit(X_train, y_train)
    y_pred = xgb.predict(X_train)
    base_error = metrics.mean_absolute_error(y_train, y_pred)
    error_list = []
    for feature_col_idx in range(len(feature_cols)):
        multi_col_df = []
        for i in range(len(feature_cols)):
            if i != feature_col_idx:
                index = [feature_cols[i] + str(j) for j in range(feature_num_col_list[0][i])]
                multi_col_df.append(train_df[feature_cols[i]].apply(pd.Series, index=index))
        X_train = pd.concat(multi_col_df, axis=1)
        y_train = train_df[label]
        # X_train, _, y_train, _ = train_test_split(X, y, test_size=1, random_state=12)
        # print(X_train.shape)
        xgb = XGBRegressor(n_estimators=n_estimators, learning_rate=learning_rate, max_depth=max_depth)
        xgb.fit(X_train, y_train)
        y_pred = xgb.predict(X_train)
        error = metrics.mean_absolute_error(y_train, y_pred) - base_error
        error_list.append(error)
    print(error_list)
    positive_error_list = [max(0.0, error) for error in error_list]
    print([float(i)/sum(error_list) for i in positive_error_list])


# xgb.feature_importances_
# 'active_frees_rate', 'frees_watching_match_rate',
# 'active_subscribers_rate', 'subscribers_watching_match_rate'
# [(80, 0.06, 7, 0.13254116978187386), (50, 0.1, 7, 0.13285655673952135), (80, 0.06, 6, 0.13296070707921204), (90, 0.06, 7, 0.13296865964147442), (80, 0.06, 5, 0.13364295045999205), (50, 0.1, 6, 0.1337916417428375), (80, 0.06, 4, 0.1342266012312262), (80, 0.06, 3, 0.1345112342442605), (70, 0.06, 3, 0.13464011921293015), (70, 0.06, 2, 0.13511308816178755)]
# [(90, 0.1, 2, 0.4634771414843388), (80, 0.1, 2, 0.4650745520497132), (70, 0.1, 2, 0.4740912203340653), (70, 0.2, 2, 0.477397446403478), (50, 0.2, 2, 0.4777133977109536), (60, 0.2, 2, 0.4781620815897597), (80, 0.2, 2, 0.4787224562947687), (90, 0.2, 2, 0.4804558183105606), (40, 0.2, 2, 0.48430628993726826), (60, 0.1, 2, 0.486101865908486)]
# [(90, 0.1, 2, 0.11998065350033846), (50, 0.2, 2, 0.12021094498673594), (80, 0.1, 2, 0.12023702205992355), (60, 0.2, 2, 0.12028915830934309), (70, 0.1, 2, 0.1204190456978632), (40, 0.2, 2, 0.12091239524301903), (60, 0.1, 2, 0.12138624097726365), (30, 0.2, 2, 0.12151276216525106), (70, 0.2, 2, 0.12167094119909118), (50, 0.1, 2, 0.12212174183529528)]
# [(90, 0.2, 2, 0.32995555289615675), (80, 0.2, 2, 0.3306924518793546), (70, 0.2, 2, 0.3319449345354321), (60, 0.2, 2, 0.3352959984406875), (40, 0.2, 2, 0.3402108953858969), (50, 0.2, 2, 0.3402156009816948), (90, 0.1, 2, 0.3428520068821816), (30, 0.2, 2, 0.3437507657452456), (60, 0.1, 2, 0.34445259317250554), (90, 0.06, 2, 0.3454314784031953)]
# draw prediction value and real value figure

# active_frees_rate
# [9.064154031396179e-05, 2.9342328555731143e-05, 4.894681316214462e-06, 0.0, 0.0, 0.0, 0.0, 0.0, 1.939054296798068e-05, 0.0012043900736447071, 0.0004950961860188475, 1.474942419070046e-05]
# [0.048771217289997115, 0.0157881372804255, 0.0026336662551354937, 0.0, 0.0, 0.0, 0.0, 0.0, 0.010433410345705944, 0.6480424955288829, 0.2663948955869045, 0.00793617771294856]
# frees_watching_match_rate
# [4.070444202069687e-05, 0.00047322943361127745, 4.704813317178702e-05, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0015542572373242504, 0.0037606042189583105, -0.0006286759135246504, 0.0]
# [0.007757412284001172, 0.09018759720574084, 0.008966386666609202, 0.0, 0.0, 0.0, 0.0, 0.0, 0.29620880638005725, 0.7166922309997653, 0.0, 0.0]
# active_subscribers_rate
# [-1.4924926331439867e-05, 0.00027859583267295963, -8.399333186660113e-05, 0.0, 0.0, 0.0, 0.0, 0.0, -3.1557382517843555e-05, 0.002648699546333691, 0.0014770733303635444, 0.0]
# [0.0, 0.06518549439531932, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.6197393111586827, 0.34560371694760716, 0.0]
# subscribers_watching_match_rate
# [-0.0002975682358391622, 0.004454138145416113, 6.7002686423012214e-06, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0025836485493788176, 0.01030998633161322, -1.4343462853057959e-05, 0.0]
# [0.0, 0.26135379474690607, 0.0003931491521634271, 0.0, 0.0, 0.0, 0.0, 0.0, 0.15159977769603067, 0.6049552042585151, 0.0, 0.0]
