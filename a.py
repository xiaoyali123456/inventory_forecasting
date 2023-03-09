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

storageLevel = StorageLevel.DISK_ONLY
distribution_fun = "default"
# distribution_fun = "gaussian"
valid_creative_dic = {}


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


check_title_valid_udf = F.udf(check_title_valid, IntegerType())


concurrency_root_path = "s3://hotstar-dp-datalake-processed-us-east-1-prod/hive_internal_database/concurrency.db/"
ssai_concurrency_path = f"{concurrency_root_path}/users_by_live_sports_content_by_ssai"
match_meta_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/ads_crash/match_meta"
live_ads_inventory_forecasting_root_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting"
play_out_log_input_path = "s3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test"
watchAggregatedInputPath = "s3://hotstar-dp-datalake-processed-us-east-1-prod/aggregates/watched_video_daily_aggregates_ist_v4"
viewAggregatedInputPath = "s3://hotstar-dp-datalake-processed-us-east-1-prod/aggregates/viewed_page_daily_aggregates_ist_v2"

tournament_dic = {"wc2022": ['"ICC Men\'s T20 World Cup 2022"'],
                  "ac2022": ['"DP World Asia Cup 2022"'],
                  "ipl2022": ['"TATA IPL 2022"'],
                  "wc2021": ['"ICC Men\'s T20 World Cup 2021"'],
                  "ipl2021": ['"VIVO IPL 2021"'],
                  "ipl2020": ['"Dream11 IPL 2020"'],
                  "wc2019": ['"ICC CWC 2019"'],
                  "west_indies_tour_of_india2019": ['"West Indies Tour India 2019"', '"West Indies Tour of India 2019"'],
                  "australia_tour_of_india2020": ['"Australia Tour of India 2020"', '"Australia tour of India 2020"'],
                  "india_tour_of_new_zealand2020": ['"India Tour of New Zealand 2020"', '"India tour of New Zealand 2020"'],
                  "england_tour_of_india2021": ['"England Tour of India 2021"'],
                  "south_africa_tour_of_india2022": ['"South Africa Tour of India 2022"'],
                  "west_indies_tour_of_india2022": ['"West Indies Tour of India 2022"'],
                  "sri_lanka_tour_of_india2023": ['"Sri Lanka Tour of India 2023"'],
                  "new_zealand_tour_of_india2023": ['"New Zealand Tour of India 2023"'],
                  "ipl2019": ['"VIVO IPL 2019"']}

tournament_dic_2 = {}
for tournament in tournament_dic:
    if tournament == "england_tour_of_india2021":
        tournament_dic_2[tournament] = "1540005184|1540005193|1540005196|1540005199|1540005211|1540005214"
        tournament_dic_2[tournament] = ",".join("\""+contentid+"\"" for contentid in tournament_dic_2[tournament].split("|"))
        print(tournament_dic_2[tournament])
    else:
        tournament_dic_2[tournament] = ""

tournament = "wc2022"
# tournament = "ac2022"
# tournament = "ipl2022"
# tournament = "wc2021"
# tournament = "ipl2021"
# tournament = "wc2019"
# tournament = "west_indies_tour_of_india2019"
# tournament = "australia_tour_of_india2020"
# tournament = "india_tour_of_new_zealand2020"  # not solved
# tournament = "england_tour_of_india2021"
# tournament = "ipl2020"
# tournament = "south_africa_tour_of_india2022"
# tournament = "west_indies_tour_of_india2022"
# tournament = "sri_lanka_tour_of_india2023"
# tournament = "new_zealand_tour_of_india2023"
# tournament = "ipl2019"


subscription_status_col = "_col3"
dw_p_id_col = "_col0"
content_id_col = "_col2"
watch_time_col = "_col27"


match_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"match_data/{tournament}").cache()
print(match_df.count())
match_df.show(200, False)
valid_dates = match_df.select('date').orderBy('date').distinct().collect()
print(valid_dates)
print(len(valid_dates))
# match_df.show(200, False)
# if tournament == "ipl2021":
#     valid_dates = [date for date in valid_dates if date[0] >= "2021-09-21" and date[0] != "2021-09-26"][:-1]

print(valid_dates)
complete_valid_dates = [date[0] for date in valid_dates]
for date in valid_dates:
    next_date = get_date_list(date[0], 2)[-1]
    if next_date not in complete_valid_dates:
        complete_valid_dates.append(next_date)

print(complete_valid_dates)



watch_df = reduce(lambda x, y: x.union(y),
    [load_data_frame(spark, f'{watchAggregatedInputPath}/cd={date}', fmt="orc")
        .withColumnRenamed(subscription_status_col, 'subscription_status')
        .withColumnRenamed(dw_p_id_col, 'dw_p_id')
        .withColumnRenamed(content_id_col, 'content_id')
        .withColumnRenamed(watch_time_col, 'watch_time')
        .withColumnRenamed('_col70', 'login_method')
        .withColumn('subscription_status', F.upper(F.col('subscription_status')))
        .where('subscription_status not in ("ACTIVE", "CANCELLED", "GRACEPERIOD")')
        .groupBy('dw_p_id', 'login_method', 'content_id')
        .agg(F.sum('watch_time').alias('watch_time'))
        .withColumn('date', F.lit(date)) for date in complete_valid_dates]) \
    .join(match_df, ['date', 'content_id'], 'left')\
    .cache()

watch_df.groupBy('login_method').agg(F.countDistinct('dw_p_id'), F.avg('watch_time'), F.countDistinct('content_id')).show(20, False)
watch_df.where('title is not null').groupBy('login_method').agg(F.countDistinct('dw_p_id'), F.avg('watch_time'), F.countDistinct('content_id')).show(20, False)
# >>> watch_df.groupBy('login_method').agg(F.countDistinct('dw_p_id'), F.avg('watch_time'), F.countDistinct('content_id')).show(20, False)
# +------------+-----------------------+------------------+--------------------------+1) / 256]]]]]
# |login_method|count(DISTINCT dw_p_id)|avg(watch_time)   |count(DISTINCT content_id)|
# +------------+-----------------------+------------------+--------------------------+
# |null        |67510255               |9.156536150398052 |314117                    |
# |Acn         |1                      |10.919999884234535|3                         |
# |Guest       |13497043               |11.498208784853041|252300                    |
# |            |3                      |12.812222290039061|3                         |
# |Auto Login  |4848                   |9.536090373799997 |9126                      |
# |Dhruv       |1                      |11.949666848447587|3                         |
# |Email       |29746                  |7.045565188952114 |25836                     |
# |Unknown     |6264                   |8.385984762284204 |2496                      |
# |Phone Number|51019810               |10.520628191932763|318110                    |
# |Facebook    |31893                  |9.916837344293098 |38656                     |
# |Device      |196                    |4.689275431474422 |171                       |
# +------------+-----------------------+------------------+--------------------------+
#
# >>> watch_df.where('title is not null').groupBy('login_method').agg(F.countDistinct('dw_p_id'), F.avg('watch_time'), F.countDistinct('content_id')).show(20, False)
# [Stage 726:====>  (7007 + -803) / 11520][Stage 1392:=====>  (7832 + 27) / 11520]23/03/07 06:56:50 WARN AsyncEventQueue: Dropped 7961 events from appStatus since Tue Mar 07 06:52:47 UTC 2023.
# +------------+-----------------------+------------------+--------------------------+ 314) / 7168]]
# |login_method|count(DISTINCT dw_p_id)|avg(watch_time)   |count(DISTINCT content_id)|
# +------------+-----------------------+------------------+--------------------------+
# |null        |27640412               |4.193250743516496 |45                        |
# |Acn         |1                      |5.021833292643229 |1                         |
# |Guest       |73298                  |17.947477575158995|45                        |
# |Auto Login  |2196                   |4.426534550118884 |45                        |
# |Email       |4354                   |8.356603917944712 |45                        |
# |Unknown     |4428                   |5.2670585155576095|45                        |
# |Phone Number|23919327               |5.1377772673473086|45                        |
# |Facebook    |17022                  |4.328221003083624 |45                        |
# +------------+-----------------------+------------------+--------------------------+

valid_contents = match_df.select('date', 'content_id').distinct().orderBy('date').collect()
active_free_list = []
for item in valid_contents:
    num = watch_df\
        .where(f'date="{item[0]}" and (content_id = "{item[1]}" or title is null)') \
        .groupBy('date')\
        .agg(F.countDistinct('dw_p_id').alias('active_free_num'))\
        .select('active_free_num').collect()[0][0]
    active_free_list.append((item[0], item[1], num))
    print(active_free_list[-1])
    print(len(active_free_list))

active_free_df = spark.createDataFrame(active_free_list, ['date', 'content_id', 'active_free_num']).cache()
save_data_frame(active_free_df, live_ads_inventory_forecasting_root_path + f"/active_free_table_for_{tournament}")

active_free_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/active_free_table_for_{tournament}")
match_active_free_df = watch_df\
    .groupBy('content_id')\
    .agg(F.countDistinct('dw_p_id').alias('match_active_free_num'),
         F.sum('watch_time').alias('total_free_watch_time'))\
    .withColumn('avg_watch_time', F.expr('total_free_watch_time/match_active_free_num'))\
    .cache()

res_df = free_num_df\
    .join(active_free_df, 'date')\
    .join(match_active_free_df, ['content_id'])\
    .withColumn('active_free_rate', F.expr('active_free_num/free_num'))\
    .withColumn('match_active_free_rate', F.expr('match_active_free_num/active_free_num'))\
    .join(match_df, ['date', 'content_id'])\
    .orderBy('content_id')\
    .cache()

save_data_frame(res_df, live_ads_inventory_forecasting_root_path + f"/free_checking_result_of_{tournament}")
# res_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/free_checking_result_of_{tournament}")
res_df.orderBy('date', 'content_id').show(200, False)
print(f"{tournament} done!")


