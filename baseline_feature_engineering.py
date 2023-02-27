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


def get_match_start_time(content_id, rank, match_start_time):
    if match_start_time:
        return match_start_time
    else:
        if content_id == "1540019005":
            return "13:30:00"
        else:
            if rank == 1:
                return "19:00:00"
            else:
                return "15:00:00"


def simple_title(title):
    if title.find(": ") > -1:
        title = title.split(": ")[-1]
    if title.find(", ") > -1:
        title = title.split(", ")[0]
    teams = sorted(title.split(" vs "))
    return teams[0] + " vs " + teams[1]


def get_teams_tier(simple_title):
    return "tier1 vs tier1"


def get_match_stage(tournament, date, rank):
    if tournament == "wc2019":
        if date >= "2019-07-09":
            return "knock-off"
        else:
            return "group"
    elif tournament == "wc2021" or tournament == "wc2022":
        if rank <= 12:
            return "qualifier"
        elif rank <= 42:
            return "group"
        else:
            return "knock-off"
    elif tournament == "ipl2021":
        if date >= "2021-10-10":
            return "knock-off"
        else:
            return "group"
    elif tournament == "ipl2022":
        if date >= "2022-05-24":
            return "knock-off"
        else:
            return "group"
    elif tournament == "ac2022":
        if date <= "2022-08-24":
            return "qualifier"
        elif date <= "2022-09-09":
            return "group"
        else:
            return "knock-off"
    else:
        return ""


def generate_hot_vector(hots, hots_num):
    res = [0 for i in range(hots_num)]
    for hot in hots:
        res[hot] = 1
    return res


def plaform_process(platforms):
    res = []
    for platform in platforms:
        if platform.endswith("ndroid"):
            res.append("android")
        else:
            res.append(platform)
    return list(set(res))


def languages_process(languages):
    res = []
    for language in languages:
        if language.find("[") > -1:
            items = language.replace("[", "").replace("]", "").replace('"', "").split(",")
            res += items
        else:
            if language.startswith("ben"):
                res.append("bengali")
            elif language.startswith("dug"):
                res.append("dugout")
            elif language.startswith("eng"):
                res.append("english")
            elif language.startswith("guj"):
                res.append("gujarati")
            elif language.startswith("hin"):
                res.append("hindi")
            elif language.startswith("kan"):
                res.append("kannada")
            elif language.startswith("mal"):
                res.append("malayalam")
            elif language.startswith("mar"):
                res.append("marathi")
            elif language.startswith("tam"):
                res.append("tamil")
            elif language.startswith("tel"):
                res.append("telugu")
            else:
                res.append(language)
    return list(set(res))


check_title_valid_udf = F.udf(check_title_valid, IntegerType())
get_match_start_time_udf = F.udf(get_match_start_time, StringType())
strip_udf = F.udf(lambda x: x.strip(), StringType())
if_holiday_udf = F.udf(lambda x: 1 if x in india_holidays else 0, IntegerType())
get_venue_udf = F.udf(lambda x, y: (tournament_dic[x][3].lower().split(", ")[0] if y < "2021-06-01" else tournament_dic[x][3].lower().split(", ")[1]) if x == "ipl2021" else tournament_dic[x][3].lower(), StringType())
get_match_type_udf = F.udf(lambda x: tournament_dic[x][2].lower(), StringType())
get_tournament_name_udf = F.udf(lambda x: " ".join(tournament_dic[x][1].lower().split(" ")[:-1]), StringType())
get_gender_type_udf = F.udf(lambda x: tournament_dic[x][4].lower(), StringType())
get_year_udf = F.udf(lambda x: int(tournament_dic[x][1].lower().split(" ")[-1]), IntegerType())
simple_title_udf = F.udf(simple_title, StringType())
get_teams_tier_udf = F.udf(get_teams_tier, StringType())
get_match_stage_udf = F.udf(get_match_stage, StringType())
generate_hot_vector_udf = F.udf(generate_hot_vector, ArrayType(IntegerType()))
plaform_process_udf = F.udf(plaform_process, ArrayType(StringType()))
languages_process_udf = F.udf(languages_process, ArrayType(StringType()))

concurrency_root_path = "s3://hotstar-dp-datalake-processed-us-east-1-prod/hive_internal_database/concurrency.db/"
ssai_concurrency_path = f"{concurrency_root_path}/users_by_live_sports_content_by_ssai"
match_meta_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/ads_crash/match_meta"
live_ads_inventory_forecasting_root_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting"
play_out_log_input_path = "s3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test/"
play_out_log_v2_input_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/playout_v2/cd='
play_out_log_wc2019_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/playout_log_v2/wc2019/'
watchAggregatedInputPath = "s3://hotstar-dp-datalake-processed-us-east-1-prod/aggregates/watched_video_daily_aggregates_ist_v4"
viewAggregatedInputPath = "s3://hotstar-dp-datalake-processed-us-east-1-prod/aggregates/viewed_page_daily_aggregates_ist_v2"

# spark.stop()
# spark = hive_spark('statistics')
# match_df = load_hive_table(spark, "in_cms.match_update_s3")
# save_data_frame(match_df, match_meta_path)
tournament_dic = {"wc2022": ["ICC Men\'s T20 World Cup 2022", "World Cup 2022", "T20", "Australia", "men", "svod"],
                  "ac2022": ["DP World Asia Cup 2022", "Asia Cup 2022", "T20", "United Arab Emirates", "men", "svod"],
                  "ipl2022": ["TATA IPL 2022", "IPL 2022", "T20", "India", "men", "svod"],
                  "wc2021": ["ICC Men\'s T20 World Cup 2021", "World Cup 2021", "T20", "United Arab Emirates", "men", "svod"],
                  "ipl2021": ["VIVO IPL 2021", "IPL 2021", "T20", "India, United Arab Emirates", "men", "svod"],
                  "wc2019": ["ICC CWC 2019", "World Cup 2019", "ODI", "England", "men", "avod"]}

india_holidays = ["2019-1-26", "2019-3-4", "2019-3-21", "2019-4-17", "2019-4-19", "2019-5-18", "2019-6-5",
                  "2019-8-12", "2019-8-15", "2019-8-24", "2019-9-10", "2019-10-2", "2019-10-8", "2019-10-27",
                  "2019-11-10", "2019-11-12", "2019-12-25",
                  "2020-1-26", "2020-3-10", "2020-4-2", "2020-4-6", "2020-4-10", "2020-5-7", "2020-5-25",
                  "2020-8-1", "2020-8-11", "2020-8-15", "2020-8-30", "2020-10-2",
                  "2020-10-25", "2020-10-30", "2020-11-14", "2020-11-30", "2020-12-25",
                  "2021-1-26", "2021-3-29", "2021-4-2", "2021-4-21", "2021-4-25", "2021-5-14",
                  "2021-5-26", "2021-7-21", "2021-8-15", "2021-8-19", "2021-8-30", "2021-10-2",
                  "2021-10-15", "2021-10-19", "2021-11-4", "2021-11-19", "2021-12-25",
                  "2022-1-26", "2022-3-1", "2022-3-18", "2022-4-14", "2022-4-15", "2022-5-3",
                  "2022-5-16", "2022-7-10", "2022-8-9", "2022-8-15", "2022-8-19", "2022-10-2",
                  "2022-10-5", "2022-10-9", "2022-10-24", "2022-11-8", "2022-12-25",
                  "2023-1-26", "2023-3-8", "2023-3-30", "2023-4-4", "2023-4-7", "2023-4-22",
                  "2023-5-5", "2023-6-29", "2023-7-29", "2023-8-15", "2023-8-19", "2023-9-7",
                  "2023-9-28", "2023-10-2", "2023-10-24", "2023-11-12", "2023-11-27", "2023-12-25"]
for i in range(len(india_holidays)):
    items = india_holidays[i].split("-")
    if len(items[1]) == 1:
        items[1] = "0" + items[1]
    if len(items[2]) == 1:
        items[2] = "0" + items[2]
    india_holidays[i] = "-".join(items)

one_hot_cols = ['if_weekend', 'match_time', 'if_holiday', 'venue', 'if_contain_india_team', 'match_type', 'tournament_name', 'gender_type', 'hostar_influence', 'match_stage']
multi_hot_cols = ['teams', 'teams_tier']
additional_cols = ["languages", "platforms"]


def get_language_and_platform(tournament):
    match_df = load_data_frame(spark, match_meta_path) \
        .withColumn('date', F.expr('substring(from_unixtime(startdate), 1, 10)')) \
        .where(f'shortsummary="{tournament_dic[tournament][0]}" and contenttype="SPORT_LIVE"') \
        .withColumn('date', F.expr('if(contentid="1540019056", "2022-11-06", date)')) \
        .withColumn('title', F.expr('lower(title)')) \
        .withColumn('title_valid_tag', check_title_valid_udf('title', F.lit('warm-up'), F.lit('follow on'),
                                                             F.lit(' fuls '), F.lit('live commentary'), F.lit('hotstar'))) \
        .where('title_valid_tag = 1') \
        .selectExpr('date', 'contentid as content_id', 'title', 'shortsummary') \
        .orderBy('date') \
        .distinct() \
        .cache()
    valid_dates = match_df.select('date', 'content_id').distinct().collect()
    content_id_col = "_col2"
    language_col = "_col16"
    platform_col = "_col23"
    watch_df = reduce(lambda x, y: x.union(y),
                      [load_data_frame(spark, f'{watchAggregatedInputPath}/cd={date[0]}', fmt="orc")
                      .withColumnRenamed(content_id_col, 'content_id')
                      .withColumnRenamed(language_col, 'language')
                      .withColumnRenamed(platform_col, 'platform')
                      .where(f'content_id = "{date[1]}"')
                      .select('content_id', 'language', 'platform')
                      .withColumn('language', F.expr('lower(language)'))
                      .withColumn('platform', F.expr('lower(platform)'))
                      .distinct() for date in valid_dates]) \
        .groupBy('content_id') \
        .agg(F.collect_list('language').alias('languages'), F.collect_list('platform').alias('platforms'))\
        .withColumn('languages', F.array_distinct(F.col('languages')))\
        .withColumn('platforms', F.array_distinct(F.col('platforms')))\
        .cache()
    save_data_frame(watch_df, live_ads_inventory_forecasting_root_path + f"/language_and_platform_of_{tournament}")


def get_playout_df(tournament):
    if tournament == "wc2019":
        return load_data_frame(spark, play_out_log_wc2019_path)\
            .selectExpr('content_id', 'break_ist_date as start_date', 'break_ist_start_time as start_time') \
            .withColumn('start_time', F.concat_ws(" ", F.col('start_date'), F.col('start_time'))) \
            .select('content_id', 'start_time') \
            .withColumn('tournament', F.lit(tournament))
    match_df = load_data_frame(spark, match_meta_path) \
        .withColumn('date', F.expr('substring(from_unixtime(startdate), 1, 10)')) \
        .where(f'shortsummary="{tournament_dic[tournament][0]}" and contenttype="SPORT_LIVE"') \
        .withColumn('date', F.expr('if(contentid="1540019056", "2022-11-06", date)')) \
        .withColumn('title', F.expr('lower(title)')) \
        .withColumn('title_valid_tag', check_title_valid_udf('title', F.lit('warm-up'), F.lit('follow on'),
                                                             F.lit(' fuls '), F.lit('live commentary'), F.lit('hotstar'))) \
        .where('title_valid_tag = 1') \
        .selectExpr('date', 'contentid as content_id', 'title', 'shortsummary') \
        .orderBy('date') \
        .distinct() \
        .cache()
    valid_dates = match_df.select('date').distinct().collect()
    if tournament == "ipl2021":
        valid_dates = [date for date in valid_dates if date[0] >= "2021-09-21" and date[0] != "2021-09-26"][:-1]
    content_id_col = "Content ID"
    start_time_col = "Start Time"
    content_id_col2 = "_c4"
    start_time_col2 = "_c2"
    if tournament == "wc2021":
        play_out_log_input_path_final = play_out_log_v2_input_path
    else:
        play_out_log_input_path_final = play_out_log_input_path
    return reduce(lambda x, y: x.union(y),
                        [load_data_frame(spark, f"{play_out_log_input_path_final}{date[0]}", 'csv', True)
                            .withColumn('date', F.lit(date[0]))
                            .withColumnRenamed(content_id_col, 'content_id')
                            .withColumnRenamed(start_time_col, 'start_time')
                            .withColumnRenamed(content_id_col2, 'content_id')
                            .withColumnRenamed(start_time_col2, 'start_time')
                            .select('content_id', 'start_time', 'date') for date in valid_dates]) \
        .withColumn('content_id', F.trim(F.col('content_id'))) \
        .withColumn('tournament', F.lit(tournament))\
        .where('start_time is not null')\
        .withColumn('start_time', strip_udf('start_time')) \
        .withColumn('start_time', F.expr('if(length(start_time)==7 and tournament="ac2022", concat_ws("", "0", start_time), start_time)')) \
        .withColumn('start_time', F.expr('if(content_id="1540017117", concat_ws(" ", start_time, "pm"), start_time)'))\
        .withColumn('start_time', F.expr('if(length(start_time)==11 and substring(start_time, 1, 8) >= "13:00:00" and tournament = "ac2022", substring(start_time, 1, 8), start_time)'))\
        .withColumn('start_time', F.expr('if(length(start_time)==8, start_time, from_unixtime(unix_timestamp(start_time, "hh:mm:ss aa"), "HH:mm:ss"))'))\
        .withColumn('next_date', F.date_add(F.col('date'), 1)) \
        .withColumn('start_date', F.expr('if(start_time < "03:00:00", next_date, date)')) \
        .withColumn('start_time', F.concat_ws(" ", F.col('start_date'), F.col('start_time')))\
        .select('content_id', 'start_time', 'tournament')


def main(tournament):
    df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/free_checking_result_of_{tournament}")\
            .join(load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/sub_checking_result_of_{tournament}")
                  .withColumnRenamed('avg_watch_time', 'avg_sub_watch_time').withColumnRenamed('total_watch_time', 'total_sub_watch_time'),
                  ['date', 'content_id', 'title', 'shortsummary'])\
            .withColumn("if_contain_india_team", F.locate('india', F.col('title')))\
            .withColumn('if_contain_india_team', F.expr('if(if_contain_india_team > 0, 1, 0)'))\
            .withColumn('if_weekend', F.dayofweek(F.col('date')))\
            .withColumn('if_weekend', F.expr('if(if_weekend=1 or if_weekend = 7, 1, 0)'))\
            .withColumnRenamed('sub_num', 'total_subscribers_number')\
            .withColumnRenamed('active_sub_rate', 'active_subscribers_rate')\
            .withColumnRenamed('match_active_sub_rate', 'subscribers_watching_match_rate')\
            .withColumnRenamed('avg_sub_watch_time', 'watch_time_per_subscriber_per_match') \
            .withColumnRenamed('free_num', 'total_frees_number') \
            .withColumnRenamed('active_free_rate', 'active_frees_rate') \
            .withColumnRenamed('match_active_free_rate', 'frees_watching_match_rate') \
            .withColumnRenamed('avg_watch_time', 'watch_time_per_free_per_match')
    return df\
        .selectExpr('date', 'content_id', 'title', 'shortsummary',
                    'total_frees_number', 'active_free_num', 'match_active_free_num', 'total_free_watch_time',
                    'total_subscribers_number', 'active_sub_num', 'match_active_sub_num', 'total_sub_watch_time',
                    'active_frees_rate', 'frees_watching_match_rate', 'watch_time_per_free_per_match',
                    'active_subscribers_rate', 'subscribers_watching_match_rate', 'watch_time_per_subscriber_per_match',
                    'if_contain_india_team', 'if_weekend')\
        .orderBy('date', 'content_id')\
        .withColumn('tournament', F.lit(tournament))


def merge_base_features():
    df = main("wc2019")\
        .union(main("wc2021"))\
        .union(main("wc2022"))\
        .union(main("ipl2021"))\
        .union(main("ipl2022"))\
        .union(main("ac2022"))
    save_data_frame(df, live_ads_inventory_forecasting_root_path + "/baseline_features")


def get_match_time():
    df = reduce(lambda x, y: x.union(y),
                        [get_playout_df(tournament[0]) for tournament in tournament_dic])\
        .cache()
    playout_df = df\
        .groupBy('tournament', 'content_id')\
        .agg(F.min('start_time').alias('match_start_time'))\
        .withColumn('match_start_time', F.from_unixtime(
                F.unix_timestamp(F.col('match_start_time'), 'yyyy-MM-dd HH:mm:ss') + F.lit(3620)))\
        .withColumn('match_start_time', F.substring(F.col('match_start_time'), 12, 8))\
        .cache()
    feature_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + "/baseline_features")\
        .cache()
    feature_df.groupBy('tournament').count().show()
    res_df = feature_df\
        .join(playout_df, ['tournament', 'content_id'], 'left')\
        .withColumn('rank', F.expr('row_number() over (partition by date order by content_id desc)'))\
        .withColumn('match_start_time', get_match_start_time_udf('content_id', 'rank', 'match_start_time')) \
        .withColumn('match_time', F.expr('if(match_start_time < "06:00:00", 0, '
                                         'if(match_start_time < "12:00:00", 1, '
                                         'if(match_start_time < "18:00:00", 2, 3)))')) \
        .orderBy('date', 'content_id')
    # res_df.groupBy('tournament').count().show()
    # res_df.where('match_time is null').orderBy('date').show(200, False)
    res_df.show(2000, False)
    save_data_frame(res_df, live_ads_inventory_forecasting_root_path + "/baseline_features_with_match_start_time")


def get_holiday_tag():
    df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + "/baseline_features_with_match_start_time")\
        .withColumn('if_holiday', if_holiday_udf('date'))
    save_data_frame(df, live_ads_inventory_forecasting_root_path + "/baseline_features_with_match_start_time_and_if_holiday")
    df.select('date', 'if_holiday').orderBy('date').show(2000)


def get_language_and_platform_all():
    for tournament in tournament_dic:
        print(tournament_dic[tournament][0])
        get_language_and_platform(tournament)
    df = reduce(lambda x, y: x.union(y),
                          [load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/language_and_platform_of_{tournament}")
                           for tournament in tournament_dic])
    save_data_frame(df, live_ads_inventory_forecasting_root_path + f"/language_and_platform_all")


def add_more_features():
    col_num_dic = {}
    df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + "/baseline_features_with_match_start_time_and_if_holiday")\
        .withColumn('venue', get_venue_udf('tournament', 'date'))\
        .withColumn('match_type', get_match_type_udf('tournament'))\
        .withColumn('tournament_name', get_tournament_name_udf('tournament'))\
        .withColumn('gender_type', get_gender_type_udf('tournament'))\
        .withColumn('year', get_year_udf('tournament'))\
        .withColumn('hostar_influence', F.expr('year - 2016'))\
        .withColumn('rank', F.expr('row_number() over (partition by tournament order by date)'))\
        .withColumn('match_stage', get_match_stage_udf('tournament', 'date', 'rank')) \
        .withColumn('simple_title', simple_title_udf('title'))\
        .withColumn('teams_tier', get_teams_tier_udf('simple_title'))\
        .cache()
    save_data_frame(df, live_ads_inventory_forecasting_root_path + "/baseline_features_with_all_features")
    df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + "/baseline_features_with_all_features")\
        .withColumn('rank', F.expr('row_number() over (partition by tournament order by date)'))\
        .withColumnRenamed('simple_title', 'teams')\
        .cache()
    df.groupBy('tournament').count().orderBy('tournament').show()
    # ['tournament', 'content_id', 'date', 'title', 'shortsummary', 'total_frees_number', 'active_free_num', 'match_active_free_num', 'total_free_watch_time',
    # 'total_subscribers_number', 'active_sub_num', 'match_active_sub_num', 'total_sub_watch_time', 'active_frees_rate', 'frees_watching_match_rate',
    # 'watch_time_per_free_per_match', 'active_subscribers_rate', 'subscribers_watching_match_rate', 'watch_time_per_subscriber_per_match',
    # 'if_contain_india_team', 'if_weekend', 'match_start_time', 'rank', 'match_time', 'if_holiday', 'venue', 'match_type', 'tournament_name',
    # 'gender_type', 'year', 'hostar_influence', 'match_stage', 'teams', 'teams_tier']
    for col in multi_hot_cols:
        print(col)
        df2 = df\
            .select('tournament', 'rank', f"{col}")\
            .withColumn(f"{col}_list", F.split(F.col(col), " vs "))\
            .withColumn(f"{col}_item", F.explode(F.col(f"{col}_list")))
        col_df = df2\
            .select(f"{col}_item") \
            .distinct() \
            .withColumn('tag', F.lit(1)) \
            .withColumn(f"{col}_hots", F.expr(f'row_number() over (partition by tag order by {col}_item)')) \
            .withColumn(f"{col}_hots", F.expr(f'{col}_hots - 1')) \
            .drop('tag')\
            .cache()
        col_num = col_df.count()
        print(col_num)
        col_num_dic[col] = col_num
        df = df\
            .join(df2
                  .select('tournament', 'rank', f"{col}_item")
                  .join(col_df, f"{col}_item")
                  .groupBy('tournament', 'rank')
                  .agg(F.collect_list(f"{col}_hots").alias(f"{col}_hots")), ['tournament', 'rank']) \
            .withColumn(f"{col}_hots_num", F.lit(col_num))
    for col in multi_hot_cols:
        print(col)
        df = df\
            .withColumn(f"{col}_hot_vector", generate_hot_vector_udf(f"{col}_hots", f"{col}_hots_num"))
    save_data_frame(df, live_ads_inventory_forecasting_root_path + "/baseline_features_with_all_features_multi_hots")
    df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + "/baseline_features_with_all_features_multi_hots")
    for col in one_hot_cols:
        print(col)
        col_df = df\
            .select(col)\
            .distinct()\
            .withColumn('tag', F.lit(1))\
            .withColumn(f"{col}_hots", F.expr(f'row_number() over (partition by tag order by {col})'))\
            .withColumn(f"{col}_hots", F.expr(f'{col}_hots - 1'))\
            .withColumn(f"{col}_hots", F.array(F.col(f"{col}_hots")))\
            .drop('tag')\
            .cache()
        col_num = col_df.count()
        print(col_num)
        col_num_dic[col] = col_num
        df = df\
            .join(col_df, col)\
            .withColumn(f"{col}_hots_num", F.lit(col_num))
    for col in one_hot_cols:
        print(col)
        df = df\
            .withColumn(f"{col}_hot_vector", generate_hot_vector_udf(f"{col}_hots", f"{col}_hots_num"))
    df.groupBy('tournament').count().orderBy('tournament').show()
    # df.where('date >= "2022-11-09" and date <= "2022-11-13"').show(20, False)
    save_data_frame(df, live_ads_inventory_forecasting_root_path + "/baseline_features_with_all_features_hots")
    df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + "/baseline_features_with_all_features_hots")\
        .cache()
    df.groupBy('tournament').count().orderBy('tournament').show()
    language_and_platform_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/language_and_platform_all")\
        .withColumn('platforms', plaform_process_udf("platforms"))\
        .withColumn('languages', languages_process_udf("languages"))\
        .cache()
    for col in additional_cols:
        print(col)
        df2 = language_and_platform_df\
            .select('content_id', f"{col}")\
            .withColumn(f"{col}_item", F.explode(F.col(f"{col}")))
        col_df = df2\
            .select(f"{col}_item") \
            .distinct() \
            .withColumn('tag', F.lit(1)) \
            .withColumn(f"{col}_hots", F.expr(f'row_number() over (partition by tag order by {col}_item)')) \
            .withColumn(f"{col}_hots", F.expr(f'{col}_hots - 1')) \
            .drop('tag')\
            .cache()
        col_num = col_df.count()
        col_num_dic[col] = col_num
        col_df.groupBy(f'{col}_item').count().orderBy(f"{col}_item").show(20, False)
        print(col_num)
        df = df\
            .join(df2
                  .select('content_id', f"{col}_item")
                  .join(col_df, f"{col}_item")
                  .groupBy('content_id')
                  .agg(F.collect_list(f"{col}_hots").alias(f"{col}_hots")), 'content_id') \
            .withColumn(f"{col}_hots_num", F.lit(col_num))
    for col in additional_cols:
        print(col)
        df = df\
            .withColumn(f"{col}_hot_vector", generate_hot_vector_udf(f"{col}_hots", f"{col}_hots_num"))
    language_and_platform_df.show(10, False)
    df.groupBy('tournament').count().orderBy('tournament').show()
    save_data_frame(df, live_ads_inventory_forecasting_root_path + f"/baseline_features_final/all_features_hots_format")
    cols = [col+"_hot_vector" for col in one_hot_cols]
    df.where('date = "2022-11-13"').select('content_id', *cols).show(20, False)
    cols = [col+"_hot_vector" for col in multi_hot_cols]
    df.where('date = "2022-11-13"').select('content_id', *cols).show(20, False)
    cols = [col+"_hot_vector" for col in additional_cols]
    df.where('date = "2022-11-13"').select('content_id', *cols).show(20, False)
    print(col_num_dic)
    # [content_id: string, match_stage: string, hostar_influence: int, gender_type: string, tournament_name: string, match_type: string, if_contain_india_team: int,
    # venue: string, if_holiday: int, match_time: int, if_weekend: int, tournament: string, rank: int, date: string, title: string, shortsummary: string,
    # total_frees_number: bigint, active_free_num: bigint, match_active_free_num: bigint, total_free_watch_time: double, total_subscribers_number: bigint,
    # active_sub_num: bigint, match_active_sub_num: bigint, total_sub_watch_time: double, active_frees_rate: double, frees_watching_match_rate: double,
    # watch_time_per_free_per_match: double, active_subscribers_rate: double, subscribers_watching_match_rate: double, watch_time_per_subscriber_per_match: double,
    # match_start_time: string, year: int, teams: string, teams_tier: string, teams_hots: array<int>, teams_hots_num: int, teams_tier_hots: array<int>, teams_tier_hots_num: int,
    # teams_hot_vector: array<int>, teams_tier_hot_vector: array<int>, if_weekend_hots: array<int>, if_weekend_hots_num: int, match_time_hots: array<int>, match_time_hots_num: int,
    # if_holiday_hots: array<int>, if_holiday_hots_num: int, venue_hots: array<int>, venue_hots_num: int, if_contain_india_team_hots: array<int>, if_contain_india_team_hots_num: int,
    # match_type_hots: array<int>, match_type_hots_num: int, tournament_name_hots: array<int>, tournament_name_hots_num: int, gender_type_hots: array<int>, gender_type_hots_num: int,
    # hostar_influence_hots: array<int>, hostar_influence_hots_num: int, match_stage_hots: array<int>, match_stage_hots_num: int, if_weekend_hot_vector: array<int>, match_time_hot_vector: array<int>,
    # if_holiday_hot_vector: array<int>, venue_hot_vector: array<int>, if_contain_india_team_hot_vector: array<int>, match_type_hot_vector: array<int>, tournament_name_hot_vector: array<int>,
    # gender_type_hot_vector: array<int>, hostar_influence_hot_vector: array<int>, match_stage_hot_vector: array<int>, languages_hots: array<int>, languages_hots_num: int, platforms_hots: array<int>,
    # platforms_hots_num: int, languages_hot_vector: array<int>, platforms_hot_vector: array<int>]


add_more_features()

cols = [col+"_hot_vector" for col in one_hot_cols+multi_hot_cols+additional_cols]
df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/baseline_features_final/all_features_hots_format")\
    .drop(*cols)
df.orderBy('date', 'content_id').show(300, False)


# save_data_frame(df, live_ads_inventory_forecasting_root_path + f"/baseline_features_final/all_features_hots_format_csv", "csv", True, '###')


