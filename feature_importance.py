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
from sklearn import metrics, linear_model

from xgboost import XGBRegressor, XGBClassifier
# pip3 install xgboost==1.6.2
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


def load_dataset(tournaments, feature_df, sorting=False, repeat_num_col="", mask_col="", mask_condition="", mask_rate=1):
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
    if repeat_num_col != "":
        new_feature_df = new_feature_df\
            .withColumn(repeat_num_col, n_to_array(repeat_num_col))
    if mask_col != "":
        new_feature_df = new_feature_df \
            .withColumn('rank_tmp', F.expr('row_number() over (partition by content_id order by date)'))\
            .withColumn("empty_"+mask_col, mask_array(mask_col))\
            .withColumn(mask_col, F.expr(f'if({mask_condition} and rank_tmp <= {mask_rate}, empty_{mask_col}, {mask_col})'))
    if sorting:
        df = new_feature_df\
            .orderBy('date', 'content_id') \
            .toPandas()
    else:
        df = new_feature_df \
            .toPandas()
    return df


def load_dataset_with_inventory(tournaments, feature_df, sorting=False):
    tournaments_str = ",".join([f'"{tournament}"' for tournament in tournaments])
    # print(tournaments_str)
    # print(new_feature_df.count())
    label_df = reduce(lambda x, y: x.union(y), [load_labels(tournament)
                      .select('content_id', 'total_inventory') for tournament in tournaments])
    estimated_free_sub_number_df = reduce(lambda x, y: x.union(y),
                                          [load_data_frame(spark, live_ads_inventory_forecasting_root_path+f"/free_and_sub_number_prediction/test_tournament={tournament}") for tournament in tournaments])
    estimated_free_sub_rate_df = reduce(lambda x, y: x.union(y),
                                          [
                                              reduce(lambda a, b: a.join(b, ['date', 'content_id']),
                                                     [load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/xgb_prediction/{tournament}/{label}").drop('real_'+label)
                                                      for label in label_cols])
                                           for tournament in tournaments])
    total_free_and_sub_wt_prediction_by_baseline_df = reduce(lambda x, y: x.union(y),
                                          [load_data_frame(spark, live_ads_inventory_forecasting_root_path+f"/total_free_and_sub_wt_prediction_by_baseline/test_tournament={tournament}")
                                           for tournament in tournaments])
    # estimated_free_sub_number_df.printSchema()
    # estimated_free_sub_rate_df.printSchema()
    # total_free_and_sub_wt_prediction_by_baseline_df.printSchema()
    new_feature_df = feature_df \
        .where(f'date != "2022-08-24" and tournament in ({tournaments_str})')\
        .join(label_df, 'content_id') \
        .join(estimated_free_sub_number_df, ['date', 'content_id']) \
        .join(estimated_free_sub_rate_df, ['date', 'content_id']) \
        .join(total_free_and_sub_wt_prediction_by_baseline_df, ['date', 'content_id']) \
        .withColumn('estimated_free_num', F.expr('cast(estimated_free_num as float)'))\
        .withColumn('estimated_sub_num', F.expr('cast(estimated_sub_num as float)'))
    # new_feature_df.printSchema()
    new_feature_df = new_feature_df\
        .withColumn('estimated_total_free_watch_time_by_xgb', F.expr('estimated_free_num * estimated_free_rate * estimated_free_watch_rate * estimated_free_watch_time'))\
        .withColumn('estimated_total_sub_watch_time_by_xgb', F.expr('estimated_sub_num * estimated_sub_rate * estimated_sub_watch_rate * estimated_sub_watch_time'))\
        .withColumn('estimated_total_watch_time_by_xgb', F.expr('estimated_total_free_watch_time_by_xgb + estimated_total_sub_watch_time_by_xgb'))\
        .withColumn('estimated_watch_time_rate_by_xgb', F.expr('estimated_total_free_watch_time_by_xgb / estimated_total_sub_watch_time_by_xgb'))\
        .withColumn('estimated_total_watch_time', F.expr('estimated_total_free_watch_time + estimated_total_sub_watch_time'))\
        .withColumn('estimated_watch_time_rate', F.expr('estimated_total_free_watch_time / estimated_total_sub_watch_time'))\
        .cache()
    if sorting:
        new_feature_df = new_feature_df\
            .orderBy('date', 'content_id')
    return new_feature_df.toPandas()


n_to_array = F.udf(lambda n: [n] * n, ArrayType(IntegerType()))
mask_array = F.udf(lambda l: [0 for i in l], ArrayType(IntegerType()))


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

tournament_list = [tournament for tournament in tournament_dic]
# tournament_list.remove('wc2019')
# total_match_duration_in_minutes, number_of_ad_breaks, average_length_of_a_break_in_seconds = 240.0, 50.0, 70.0
total_match_duration_in_minutes, number_of_ad_breaks, average_length_of_a_break_in_seconds = 210.0, 85.0, 30.0
drop_off_rate = 0.8
dynamic_parameters = ['active_frees_rate', 'frees_watching_match_rate', "watch_time_per_free_per_match",
                      'active_subscribers_rate', 'subscribers_watching_match_rate', "watch_time_per_subscriber_per_match"]
dynamic_parameter_num = len(dynamic_parameters)
one_hot_cols = ['if_weekend', 'match_time', 'if_holiday', 'venue', 'if_contain_india_team', 'match_type', 'tournament_name', 'hostar_influence', 'match_stage', 'gender_type']
multi_hot_cols = ['teams', 'teams_tier']
additional_cols = ["languages", "platforms"]
feature_cols = [col+"_hot_vector" for col in one_hot_cols[:-1]+multi_hot_cols[:-1]+additional_cols]
# feature_cols = [col+"_hot_vector" for col in one_hot_cols[:-1]+multi_hot_cols[:-1]]
# feature_cols = [col+"_hot_vector" for col in ['teams', 'match_time', 'match_stage', 'if_holiday',
#                                               'if_weekend', 'hostar_influence', 'if_contain_india_team']]
feature_num_cols = [col.replace("_hot_vector", "_hots_num") for col in feature_cols]
feature_num = len(feature_cols)
# version = "baseline"

sub_pid_did_rate = 0.94
free_pid_did_rate = 1.02

label_cols = ['active_frees_rate', 'frees_watching_match_rate',
              'active_subscribers_rate', 'subscribers_watching_match_rate',
              "watch_time_per_free_per_match", "watch_time_per_subscriber_per_match"]
large_vals = [0.1, 0.2, 0.5, 0.5]
repeat_union = 0.1
knock_off_repeat_num = 4
mask_condition = 'match_stage="knock-off"'
feature_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/baseline_features_final/all_features_hots_format")\
    .withColumn(f"{label_cols[0]}_repeat_num", F.expr(f'1 + cast(({label_cols[0]} - {large_vals[0]})/{repeat_union} as int)'))\
    .withColumn(f"{label_cols[0]}_repeat_num", F.expr(f'if({label_cols[0]}_repeat_num > 0, {label_cols[0]}_repeat_num, 1)'))\
    .withColumn(f"{label_cols[1]}_repeat_num", F.expr(f'1 + cast(({label_cols[1]} - {large_vals[1]})/{repeat_union} as int)'))\
    .withColumn(f"{label_cols[1]}_repeat_num", F.expr(f'if({label_cols[1]}_repeat_num > 0, {label_cols[1]}_repeat_num, 1)'))\
    .withColumn(f"{label_cols[2]}_repeat_num", F.expr(f'1 + cast(({label_cols[2]} - {large_vals[2]})/{repeat_union} as int)'))\
    .withColumn(f"{label_cols[2]}_repeat_num", F.expr(f'if({label_cols[2]}_repeat_num > 0, {label_cols[2]}_repeat_num, 1)'))\
    .withColumn(f"{label_cols[3]}_repeat_num", F.expr(f'1 + cast(({label_cols[3]} - {large_vals[3]})/{repeat_union} as int)'))\
    .withColumn(f"{label_cols[3]}_repeat_num", F.expr(f'if({label_cols[3]}_repeat_num > 0, {label_cols[3]}_repeat_num, 1)'))\
    .withColumn(f"knock_off_repeat_num", F.expr(f'if({mask_condition}, {knock_off_repeat_num}, 1)'))\
    .cache()
# label = "total_inventory"
feature_num_col_list = feature_df.select(*feature_num_cols).distinct().collect()
res_dic = {}
best_parameter_dic = {}
s_time = time.time()

task = "rate_prediction"
# task = "inventory_prediction"
if task == "inventory_prediction":
    tournament_list.remove("ipl2021")
    # tournament_list.remove("ipl2022")
    label = "total_inventory"
    for test_tournament in tournament_list:
        print(test_tournament)
        tournament_list_tmp = tournament_list.copy()
        tournament_list_tmp.remove(test_tournament)
        train_df = load_dataset_with_inventory(tournament_list_tmp, feature_df)
        watch_time_features = ['estimated_total_free_watch_time_by_xgb', 'estimated_total_sub_watch_time_by_xgb',
                                     'estimated_watch_time_rate_by_xgb', 'estimated_total_watch_time_by_xgb']
        multi_col_df = []
        # for i in range(len(feature_cols)):
        #     index = [feature_cols[i]+str(j) for j in range(feature_num_col_list[0][i])]
        #     multi_col_df.append(train_df[feature_cols[i]].apply(pd.Series, index=index))
        multi_col_df.append(train_df[watch_time_features])
        X_train = pd.concat(multi_col_df, axis=1)
        y_train = train_df[label]
        test_df = load_dataset_with_inventory([test_tournament], feature_df, sorting=True)
        multi_col_df = []
        # for i in range(len(feature_cols)):
        #     index = [feature_cols[i] + str(j) for j in range(feature_num_col_list[0][i])]
        #     multi_col_df.append(test_df[feature_cols[i]].apply(pd.Series, index=index))
        multi_col_df.append(test_df[watch_time_features])
        X_test = pd.concat(multi_col_df, axis=1)
        y_test = test_df[label]
        model = linear_model.LinearRegression()
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        y_test_sum = y_test.sum()
        y_pred_sum = y_pred.sum()
        print(f"{y_pred_sum} / {y_test_sum} - 1= {y_pred_sum/y_test_sum - 1}")
        # spark.createDataFrame(pd.concat([y_test, pd.DataFrame(y_pred)], axis=1), ['total_inventory', 'estimated_inventory'])\
        #     .withColumn('rate', F.expr(f'estimated_inventory/total_inventory-1'))\
        #     .show(10)
elif task == "rate_prediction":
    # all 5 tournaments
    # best_setting_dic = {'active_frees_rate': ['reg:squarederror', '43', '0.2', '9', 'estimated_free_rate'],
    #                     'frees_watching_match_rate': ['reg:squarederror', '94', '0.2', '1', 'estimated_free_watch_rate'],
    #                     'active_subscribers_rate': ['reg:squarederror', '76', '0.2', '1', 'estimated_sub_rate'],
    #                     'subscribers_watching_match_rate': ['reg:squarederror', '22', '0.2', '3', 'estimated_sub_watch_rate'],
    #                     'watch_time_per_free_per_match': ['reg:squarederror', '58', '0.2', '1', 'estimated_free_watch_time'],
    #                     'watch_time_per_subscriber_per_match': ['reg:squarederror', '85', '0.1', '3', 'estimated_sub_watch_time']}
    # all 3 tournaments excluding ipl
    # best_setting_dic = {'active_frees_rate': ['reg:squarederror', '55', '0.2', '7', 'estimated_free_rate'],
    #                     'frees_watching_match_rate': ['reg:squarederror', '34', '0.2', '3', 'estimated_free_watch_rate'],
    #                     'active_subscribers_rate': ['reg:squarederror', '97', '0.2', '1', 'estimated_sub_rate'],
    #                     'subscribers_watching_match_rate': ['reg:squarederror', '22', '0.2', '3', 'estimated_sub_watch_rate'],
    #                     'watch_time_per_free_per_match': ['reg:squarederror', '58', '0.2', '1', 'estimated_free_watch_time'],
    #                     'watch_time_per_subscriber_per_match': ['reg:squarederror', '85', '0.1', '3', 'estimated_sub_watch_time']}
    # all 4 tournaments excluding ipl
    # best_setting_dic = {'active_frees_rate': ['reg:squarederror', '43', '0.2', '9', 'estimated_free_rate'],
    #                     'frees_watching_match_rate': ['reg:squarederror', '34', '0.2', '3', 'estimated_free_watch_rate'],
    #                     'active_subscribers_rate': ['reg:squarederror', '76', '0.2', '1', 'estimated_sub_rate'],
    #                     'subscribers_watching_match_rate': ['reg:squarederror', '22', '0.2', '3', 'estimated_sub_watch_rate'],
    #                     'watch_time_per_free_per_match': ['reg:squarederror', '13', '0.2', '3', 'estimated_free_watch_time'],
    #                     'watch_time_per_subscriber_per_match': ['reg:squarederror', '67', '0.1', '3', 'estimated_sub_watch_time']}
    best_setting_dic = {'active_frees_rate': ['reg:squarederror', '88', '0.2', '7', 'estimated_free_rate'],
                        'frees_watching_match_rate': ['reg:squarederror', '37', '0.2', '3', 'estimated_free_watch_rate'],
                        'active_subscribers_rate': ['reg:squarederror', '97', '0.2', '1', 'estimated_sub_rate'],
                        'subscribers_watching_match_rate': ['reg:squarederror', '94', '0.1', '3', 'estimated_sub_watch_rate'],
                        'watch_time_per_free_per_match': ['reg:squarederror', '13', '0.2', '3', 'estimated_free_watch_time'],
                        'watch_time_per_subscriber_per_match': ['reg:squarederror', '37', '0.2', '3', 'estimated_sub_watch_time']}
    # mask_tag = "_mask_knock_off"
    mask_tag = ""
    for test_tournament in tournament_list:
        if test_tournament.find("ipl2021") > -1:
            continue
        print(test_tournament)
        tournament_list_tmp = tournament_list.copy()
        tournament_list_tmp.remove(test_tournament)
        object_method = "reg:squarederror"
        # object_method = "reg:logistic"
        # object_method = "reg:pseudohubererror"
        if mask_tag == "":
            train_df = load_dataset(tournament_list_tmp, feature_df)
            test_df = load_dataset([test_tournament], feature_df, sorting=True)
        else:
            train_df = load_dataset(tournament_list_tmp, feature_df, repeat_num_col="knock_off_repeat_num",
                                    mask_col="teams_hot_vector", mask_condition=mask_condition, mask_rate=2)
            test_df = load_dataset([test_tournament], feature_df, sorting=True,
                                   mask_col="teams_hot_vector", mask_condition=mask_condition, mask_rate=1)
        # ## for data enrichment
        # train_df = train_df.reindex(train_df.index.repeat(train_df[f"knock_off_repeat_num"]))
        print(train_df)
        # ## for data enrichment
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
        for label in label_cols:
            print("")
            print(label)
            # ## for data enrichment
            # train_df = load_dataset(tournament_list, feature_df)
            # # print(train_df)
            # train_df = train_df.reindex(train_df.index.repeat(train_df[f"{label}_repeat_num"]))
            # # print(train_df)
            # multi_col_df = []
            # for i in range(len(feature_cols)):
            #     index = [feature_cols[i] + str(j) for j in range(feature_num_col_list[0][i])]
            #     multi_col_df.append(train_df[feature_cols[i]].apply(pd.Series, index=index))
            # X_train = pd.concat(multi_col_df, axis=1)
            # ## for data enrichment
            ## for prediction start
            object_method, n_estimators, learning_rate, max_depth, col_name = best_setting_dic[label]
            n_estimators = int(n_estimators)
            learning_rate = float(learning_rate)
            max_depth = int(max_depth)
            y_train = train_df[label]
            xgb = XGBRegressor(n_estimators=n_estimators, learning_rate=learning_rate,
                               max_depth=max_depth, objective=object_method)
            xgb.fit(X_train, y_train)
            y_pred = xgb.predict(X_test)
            y_test = test_df[label]
            prediction_df = spark.createDataFrame(pd.concat([test_df[['date', 'content_id']], y_test, pd.DataFrame(y_pred)], axis=1), ['date', 'content_id', 'real_'+label, col_name])\
                .withColumn(col_name, F.expr(f'cast({col_name} as float)'))
            prediction_df\
                .withColumn('tag', F.lit(1))\
                .groupBy('tag')\
                .agg(F.min(col_name), F.max(col_name))\
                .show()
            # prediction_df.where(f"{col_name} is null").show(20)
            # prediction_df.show(20)
            save_data_frame(prediction_df, live_ads_inventory_forecasting_root_path+f"/xgb_prediction{mask_tag}/{test_tournament}/{label}")
            error = metrics.mean_absolute_error(y_test, y_pred)
            y_mean = y_test.mean()
            print(error / y_mean)
            # ## for prediction end
            if label not in res_dic:
                res_dic[label] = {}
            # for n_estimators in range(10, 100, 1000):
            for n_estimators in range(1, 100, 3):
                # print(n_estimators)
                for learning_rate in [0.01, 0.05, 0.1, 0.2]:
                    # for learning_rate in [0.1]:
                    for max_depth in range(1, 10, 2):
                        # print(f"config: n_estimators={n_estimators}, max_depth={max_depth}")
                        y_train = train_df[label]
                        xgb = XGBRegressor(n_estimators=n_estimators, learning_rate=learning_rate,
                                           max_depth=max_depth, objective=object_method)
                        xgb.fit(X_train, y_train)
                        # y_pred = xgb.predict(X_train)
                        # y_mean = y_train.mean()
                        # error = metrics.mean_absolute_error(y_train, y_pred)
                        # train_error = error/y_mean
                        y_test = test_df[label]
                        y_pred = xgb.predict(X_test)
                        y_mean = y_test.mean()
                        error = metrics.mean_absolute_error(y_test, y_pred)
                        key = f"{object_method}-{n_estimators}-{learning_rate}-{max_depth}"
                        if key not in res_dic[label]:
                            res_dic[label][key] = [error / y_mean]
                        else:
                            res_dic[label][key].append(error / y_mean)
    best_setting_dic = {}
    for label in label_cols:
        print(label)
        error_list = []
        for key in res_dic[label]:
            error_list.append((key, sum(res_dic[label][key])/len(res_dic[label][key])))
        best_parameter_dic[label] = sorted(error_list, key=lambda x: x[1])[:2]
        print(best_parameter_dic[label])
        print(res_dic[label][best_parameter_dic[label][0][0]])
        best_setting_dic[label] = best_parameter_dic[label][0][0].split("-")
    print(best_setting_dic)
    print(time.time() - s_time)




key = "reg:squarederror-67-0.1-3"
for label in label_cols:
    print(label)
    print((key, sum(res_dic[label][key])/len(res_dic[label][key])))




best_parameter_dic = {'active_frees_rate': ['reg:squarederror', '43', '0.2', '9'],
                      'frees_watching_match_rate': ['reg:squarederror', '94', '0.2', '1'],
                      'active_subscribers_rate': ['reg:squarederror', '76', '0.2', '1'],
                      'subscribers_watching_match_rate': ['reg:squarederror', '22', '0.2', '3'],
                      'watch_time_per_free_per_match': ['reg:squarederror', '58', '0.2', '1'],
                      'watch_time_per_subscriber_per_match': ['reg:squarederror', '85', '0.1', '3']}



object_method = "reg:squarederror"
best_parameters = [(47, 0.1, 9), (99, 0.2, 3),
                   (63, 0.1, 9), (57, 0.2, 9)]


# object_method = "reg:pseudohubererror"
# best_parameters = [(21, 0.2, 2), (99, 0.2, 3),
#                    (77, 0.1, 5), (45, 0.2, 9)]
# best_parameters = [(87, 0.06, 8), (75, 0.2, 2),
#                    (95, 0.2, 2), (81, 0.1, 3)]


# object_method = "reg:logistic"
# best_parameters = [(99, 0.06, 6), (71, 0.2, 8),
#                    (99, 0.2, 5), (81, 0.2, 8)]
for idx in range(len(label_cols)):
    label = label_cols[idx]
    print("|"+label)
    n_estimators, learning_rate, max_depth = best_parameters[idx]
    res_list = []
    y_train = train_df[label]
    xgb = XGBRegressor(n_estimators=n_estimators, learning_rate=learning_rate,
                       max_depth=max_depth, objective=object_method)
    xgb.fit(X_train, y_train)
    y_train_pred = xgb.predict(X_train)
    y_mean = y_train.mean()
    train_error = metrics.mean_absolute_error(y_train, y_train_pred)
    train_error = train_error / y_mean
    y_test = test_df[label]
    y_test_pred = xgb.predict(X_test)
    y_mean = y_test.mean()
    test_error = metrics.mean_absolute_error(y_test, y_test_pred)
    test_error = test_error / y_mean
    print(f"|train error: {train_error}")
    print(f"|test error: {test_error}")
    spark.createDataFrame(pd.concat([y_train, pd.DataFrame(y_train_pred)], axis=1), ['label', 'prediction']).show(1000)
    spark.createDataFrame(pd.concat([y_test, pd.DataFrame(y_test_pred)], axis=1), ['label', 'prediction']).show(1000)
    print("")

feature_weights_list = []
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
                       max_depth=max_depth, objective=object_method)
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
        xgb = XGBRegressor(n_estimators=n_estimators, learning_rate=learning_rate,
                           max_depth=max_depth, objective=object_method)
        xgb.fit(X_train, y_train)
        y_pred = xgb.predict(X_train)
        error = metrics.mean_absolute_error(y_train, y_pred) - base_error
        error_list.append(error)
    # print(error_list)
    positive_error_list = [max(0.0, error) for error in error_list]
    res_list = [float(i)/sum(positive_error_list) for i in positive_error_list]
    feature_weights_list.append(sorted([(feature_cols[j], res_list[j]) for j in range(feature_num)]
                                       , key=lambda x: x[1], reverse=True))
    # print(feature_weights_list[-1])
    # break


for i in range(4):
    print(feature_weights_list[i])


# [('teams_hot_vector', 0.21663214), ('languages_hot_vector', 0.12392009), ('tournament_name_hot_vector', 0.0068638683), ('if_weekend_hot_vector', 0.0), ('match_time_hot_vector', 0.0), ('if_holiday_hot_vector', 0.0), ('venue_hot_vector', 0.0), ('if_contain_india_team_hot_vector', 0.0), ('match_type_hot_vector', 0.0), ('hostar_influence_hot_vector', 0.0), ('match_stage_hot_vector', 0.0), ('platforms_hot_vector', 0.0)]
# [('teams_hot_vector', 0.1768074), ('if_contain_india_team_hot_vector', 0.0044579306), ('platforms_hot_vector', 0.0018656878), ('venue_hot_vector', 0.0013974725), ('if_weekend_hot_vector', 0.0011602209), ('match_type_hot_vector', 0.0010621756), ('match_stage_hot_vector', 0.0009030289), ('tournament_name_hot_vector', 0.00015270569), ('match_time_hot_vector', 0.0), ('if_holiday_hot_vector', 0.0), ('hostar_influence_hot_vector', 0.0), ('languages_hot_vector', 0.0)]
# [('teams_hot_vector', 0.07960796), ('languages_hot_vector', 0.015930658), ('platforms_hot_vector', 0.009622666), ('tournament_name_hot_vector', 0.0071320743), ('match_stage_hot_vector', 0.0057516606), ('match_type_hot_vector', 0.003761015), ('venue_hot_vector', 0.0037149698), ('if_contain_india_team_hot_vector', 0.0030376627), ('if_weekend_hot_vector', 0.002833748), ('match_time_hot_vector', 0.0), ('if_holiday_hot_vector', 0.0), ('hostar_influence_hot_vector', 0.0)]
# [('tournament_name_hot_vector', 0.010289712), ('languages_hot_vector', 0.0077993786), ('teams_hot_vector', 0.0071995393), ('platforms_hot_vector', 0.0056719654), ('venue_hot_vector', 0.0053579574), ('if_contain_india_team_hot_vector', 0.004736047), ('match_type_hot_vector', 0.002780697), ('if_weekend_hot_vector', 0.0012477075), ('match_stage_hot_vector', 0.00058643153), ('match_time_hot_vector', 0.0), ('if_holiday_hot_vector', 0.0), ('hostar_influence_hot_vector', 0.0)]


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
