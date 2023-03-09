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

# tournament = "wc2022"
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
tournament = "ipl2019"


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


ready_date_dic = {}
for date in valid_dates:
    print(date[0])
    for previous_date in get_date_list(date[0], -90):
        if not check_s3_path_exist(live_ads_inventory_forecasting_root_path + f"/all_viewers/cd={previous_date}"):
        # if previous_date not in ready_date_dic:
            print(f"- {previous_date}")
            viewers_df = load_data_frame(spark, f'{viewAggregatedInputPath}/cd={previous_date}', fmt="orc")\
                .withColumnRenamed(dw_p_id_col, 'dw_p_id')\
                .select('dw_p_id')\
                .distinct()
            save_data_frame(viewers_df, live_ads_inventory_forecasting_root_path + f"/all_viewers/cd={previous_date}")
            ready_date_dic[previous_date] = 1

print("daily viewer data ready.")


for date in valid_dates:
    print(date[0])
    df = reduce(lambda x, y: x.union(y),
           [load_data_frame(spark,
                            live_ads_inventory_forecasting_root_path + f"/all_viewers/cd={previous_date}").cache()
            for previous_date in get_date_list(date[0], -90)]).distinct()
    save_data_frame(df, live_ads_inventory_forecasting_root_path + f"/all_viewers_aggr_with_90_days/cd={date[0]}")


print("all viewer data ready.")

user_meta_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/hid_pid_mapping").cache()
sub_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/sub_table_valid").cache()
free_num_list = []
for date in valid_dates:
    print(date[0])
    # if date[0] < "2020-10-30":
    #     continue
    sub_on_target_date_df = calculate_sub_num_on_target_date(sub_df, user_meta_df, date[0]).cache()
    free_num = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/all_viewers_aggr_with_90_days/cd={date[0]}")\
        .join(sub_on_target_date_df, 'dw_p_id', 'left_anti')\
        .count()
    free_num_list.append((date[0], free_num))
    print(free_num_list)

free_num_df = spark.createDataFrame(free_num_list, ["date", "free_num"]).orderBy('date')
save_data_frame(free_num_df, live_ads_inventory_forecasting_root_path + f"/free_num_table_for_{tournament}")


# save_data_frame(free_num_df, live_ads_inventory_forecasting_root_path + f"/free_num_table_for_{tournament}_partly_2")
#
# free_num_list = [('2020-10-04', 322214736), ('2020-10-05', 324376762), ('2020-10-06', 326413743),
#                  ('2020-10-07', 328343777), ('2020-10-08', 329781502), ('2020-10-09', 331107089),
#                  ('2020-10-10', 333543318), ('2020-10-11', 335508110), ('2020-10-12', 336829358),
#                  ('2020-10-13', 338488168), ('2020-10-14', 339627770), ('2020-10-15', 340742715),
#                  ('2020-10-16', 341665463), ('2020-10-17', 343554412), ('2020-10-18', 345828733),
#                  ('2020-10-19', 347493944), ('2020-10-20', 348641982), ('2020-10-21', 349716690),
#                  ('2020-10-22', 348792955), ('2020-10-23', 346939946), ('2020-10-24', 346525932),
#                  ('2020-10-25', 347060742), ('2020-10-26', 347296749), ('2020-10-27', 347665631),
#                  ('2020-10-28', 348276971), ('2020-10-29', 348889549), ('2020-10-30', 349291400),
#                  ('2020-10-31', 350003470), ('2020-11-01', 350901046), ('2020-11-02', 351455995),
#                  ('2020-11-03', 352096617), ('2020-11-05', 353380891), ('2020-11-06', 354303042),
#                  ('2020-11-08', 355505397), ('2020-11-10', 358998467)]
# free_num_df = spark.createDataFrame(free_num_list, ["date", "free_num"])\
#     .union(load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/free_num_table_for_{tournament}_partly"))\
#     .orderBy('date')
# save_data_frame(free_num_df, live_ads_inventory_forecasting_root_path + f"/free_num_table_for_{tournament}")
#


free_num_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/free_num_table_for_{tournament}").cache()
free_num_df.show(200, False)
print(free_num_df.count())

watch_df = reduce(lambda x, y: x.union(y),
    [load_data_frame(spark, f'{watchAggregatedInputPath}/cd={date}', fmt="orc")
        .withColumnRenamed(subscription_status_col, 'subscription_status')
        .withColumnRenamed(dw_p_id_col, 'dw_p_id')
        .withColumnRenamed(content_id_col, 'content_id')
        .withColumnRenamed(watch_time_col, 'watch_time')
        .withColumn('subscription_status', F.upper(F.col('subscription_status')))
        .where('subscription_status not in ("ACTIVE", "CANCELLED", "GRACEPERIOD")')
        .groupBy('dw_p_id', 'content_id')
        .agg(F.sum('watch_time').alias('watch_time'))
        .withColumn('date', F.lit(date)) for date in complete_valid_dates]) \
    .join(match_df, ['date', 'content_id'], 'left')\
    .cache()

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


