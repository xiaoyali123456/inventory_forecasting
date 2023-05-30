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


def get_match_start_time(content_id, rank, match_start_time, tournament):
    if match_start_time:
        return match_start_time
    else:
        if content_id == "1540019005":
            return "13:30:00"
        elif tournament == "england_tour_of_india2021":
            if content_id <= "1540005181" or content_id >= "1540005202":
                return "08:00:00"
            else:
                return "13:00:00"
        else:
            if rank == 1:
                return "19:00:00"
            else:
                return "15:00:00"


def simple_title(title):
    unvalid_mapping = {
        "sl": "sri lanka",
        "eng": "england",
        "ire": "ireland",
        "dc.": "dc"
    }
    title = title.strip().lower()
    if title.find(": ") > -1:
        title = title.split(": ")[-1]
    if title.find(", ") > -1:
        title = title.split(", ")[0]
    teams = sorted(title.split(" vs "))
    for i in range(len(teams)):
        if teams[i] in unvalid_mapping:
            teams[i] = unvalid_mapping[teams[i]]
    return teams[0] + " vs " + teams[1]


def get_continents(teams, tournament_type):
    teams = teams.split(" vs ")
    continent_dic = {'australia': 'OC',
                    'england': 'EU',
                    'india': 'AS',
                    'new zealand': 'OC',
                    'pakistan': 'AS',
                    'south africa': 'AF',
                    'sri lanka': 'AS',
                    'afghanistan': 'AS',
                    'bangladesh': 'AS',
                    'west indies': 'NA',
                    'zimbabwe': 'AF',
                    'hong kong': 'AS',
                    'ireland': 'EU',
                    'namibia': 'AF',
                    'netherlands': 'EU',
                    'oman': 'AS',
                    'papua new guinea': 'OC',
                    'scotland': 'EU',
                    'uae': 'AS'}
    if tournament_type == "national":
        return "AS vs AS"
    elif teams[0] in continent_dic and teams[1] in continent_dic:
        return f"{continent_dic[teams[0]]} vs {continent_dic[teams[1]]}"
    else:
        return ""


def get_teams_tier(teams):
    teams = teams.split(" vs ")
    tiers_dic = {'australia': 'tier1',
                 'england': 'tier1',
                 'india': 'tier1',
                 'new zealand': 'tier1',
                 'pakistan': 'tier1',
                 'south africa': 'tier1',
                 'sri lanka': 'tier1',
                 'afghanistan': 'tier2',
                 'bangladesh': 'tier2',
                 'west indies': 'tier2',
                 'zimbabwe': 'tier2',
                 'hong kong': 'tier3',
                 'ireland': 'tier3',
                 'namibia': 'tier3',
                 'netherlands': 'tier3',
                 'oman': 'tier3',
                 'papua new guinea': 'tier3',
                 'scotland': 'tier3',
                 'uae': 'tier3',
                 "csk": "tier1",
                 "mi": "tier1",
                 "rcb": "tier1",
                 "dc": "tier2",
                 "gt": "tier2",
                 "kkr": "tier2",
                 "kxip": "tier2",
                 "lsg": "tier2",
                 "pbks": "tier2",
                 "rr": "tier2",
                 "srh": "tier2"
                 }
    if teams[0] in tiers_dic and teams[1] in tiers_dic:
        return f"{tiers_dic[teams[0]]} vs {tiers_dic[teams[1]]}"
    else:
        return ""


def get_match_stage(tournament, date, rank):
    if tournament == "ipl2019":
        if date >= "2019-05-12":
            return "final"
        elif date >= "2019-05-07":
            return "semi-final"
        else:
            return "group"
    elif tournament == "wc2019":
        if date >= "2019-07-14":
            return "final"
        elif date >= "2019-07-09":
            return "semi-final"
        else:
            return "group"
    elif tournament == "ipl2020":
        if date >= "2020-11-10":
            return "final"
        elif date >= "2020-11-05":
            return "semi-final"
        else:
            return "group"
    elif tournament == "ipl2021":
        if date >= "2021-10-15":
            return "final"
        elif date >= "2021-10-10":
            return "semi-final"
        else:
            return "group"
    elif tournament == "wc2021" or tournament == "wc2022":
        if rank <= 12:
            return "qualifier"
        elif rank <= 42:
            return "group"
        elif rank <= 44:
            return "semi-final"
        else:
            return "final"
    elif tournament == "ipl2022":
        if date >= "2022-05-29":
            return "final"
        elif date >= "2022-05-24":
            return "semi-final"
        else:
            return "group"
    elif tournament == "ac2022":
        if date <= "2022-08-24":
            return "qualifier"
        elif date <= "2022-09-09":
            return "group"
        else:
            return "final"
    elif tournament == "wc2023":
        if date >= "2023-11-26":
            return "final"
        elif date >= "2023-11-21":
            return "semi-final"
        else:
            return "group"
    elif tournament == "ac2023":
        if date <= "2023-09-15":
            return "group"
        else:
            return "final"
    else:
        return "group"


def generate_hot_vector(hots, hots_num):
    res = [0 for i in range(hots_num)]
    for hot in hots:
        res[hot] += 1
    return res


def plaform_process(platforms):
    res = []
    for platform in platforms:
        if platform.endswith("ndroid"):
            res.append("android")
        elif platform == "mb":
            res.append("mobile")
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
            elif language.startswith("eng") or language.startswith("الإنجليزية"):
                res.append("english")
            elif language.startswith("guj"):
                res.append("gujarati")
            elif language.startswith("hin") or language.startswith("הינדי") or language.startswith("हिन्दी"):
                res.append("hindi")
            elif language.startswith("kan"):
                res.append("kannada")
            elif language.startswith("mal") or language.startswith("മലയാളം"):
                res.append("malayalam")
            elif language.startswith("mar"):
                res.append("marathi")
            elif language.startswith("tam") or language.startswith("தமிழ்"):
                res.append("tamil")
            elif language.startswith("tel") or language.startswith("తెలుగు"):
                res.append("telugu")
            elif language.startswith("unknown") or language == "":
                res.append("unknown")
            else:
                res.append(language)
    return list(set(res))


def get_match_type(tournament, date):
    match_type_list = ["t20", "odi", "test"]
    if tournament_dic[tournament]['match_type'] != "":
        return tournament_dic[tournament]['match_type'].lower()
    else:
        if tournament == "sri_lanka_tour_of_india2023":
            if date <= "2023-01-07":
                return match_type_list[0]
            else:
                return match_type_list[1]
        elif tournament == "new_zealand_tour_of_india2023":
            if date <= "2023-01-24":
                return match_type_list[1]
            else:
                return match_type_list[0]
        elif tournament == "south_africa_tour_of_india2022":
            if date <= "2022-10-04":
                return match_type_list[0]
            else:
                return match_type_list[1]
        elif tournament == "west_indies_tour_of_india2022":
            if date <= "2022-02-11":
                return match_type_list[1]
            else:
                return match_type_list[0]
        elif tournament == "england_tour_of_india2021":
            if date <= "2021-03-09":
                return match_type_list[2]
            elif date <= "2021-03-20":
                return match_type_list[0]
            else:
                return match_type_list[1]
        elif tournament == "india_tour_of_new_zealand2020":
            if date <= "2020-02-02":
                return match_type_list[0]
            elif date <= "2020-02-11":
                return match_type_list[1]
            else:
                return match_type_list[2]
        elif tournament == "west_indies_tour_of_india2019":
            if date <= "2019-12-11":
                return match_type_list[0]
            else:
                return match_type_list[1]


check_title_valid_udf = F.udf(check_title_valid, IntegerType())
get_match_start_time_udf = F.udf(get_match_start_time, StringType())
strip_udf = F.udf(lambda x: x.strip(), StringType())
if_holiday_udf = F.udf(lambda x: 1 if x in india_holidays else 0, IntegerType())
get_venue_udf = F.udf(lambda x, y: (tournament_dic[x]["venue"].lower().split(", ")[0] if y < "2021-06-01" else tournament_dic[x]["venue"].lower().split(", ")[1]) if x == "ipl2021" else tournament_dic[x]["venue"].lower(), StringType())
get_match_type_udf = F.udf(get_match_type, StringType())
get_tournament_name_udf = F.udf(lambda x: " ".join(tournament_dic[x]['tournament_name'].lower().split(" ")[:-1]), StringType())
get_gender_type_udf = F.udf(lambda x: tournament_dic[x]['gender_type'].lower(), StringType())
get_year_udf = F.udf(lambda x: int(tournament_dic[x]['tournament_name'].lower().split(" ")[-1]), IntegerType())
simple_title_udf = F.udf(simple_title, StringType())
get_teams_tier_udf = F.udf(get_teams_tier, StringType())
get_continents_udf = F.udf(get_continents, StringType())
get_match_stage_udf = F.udf(get_match_stage, StringType())
generate_hot_vector_udf = F.udf(generate_hot_vector, ArrayType(IntegerType()))
plaform_process_udf = F.udf(plaform_process, ArrayType(StringType()))
languages_process_udf = F.udf(languages_process, ArrayType(StringType()))

concurrency_root_path = "s3://hotstar-dp-datalake-processed-us-east-1-prod/hive_internal_database/concurrency.db/"
ssai_concurrency_path = f"{concurrency_root_path}/users_by_live_sports_content_by_ssai"
match_meta_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/ads_crash/match_meta"
live_ads_inventory_forecasting_root_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting"
live_ads_inventory_forecasting_complete_feature_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/complete_features"
play_out_log_input_path = "s3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test/"
play_out_log_v2_input_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/playout_v2/cd='
play_out_log_wc2019_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/playout_log_v2/wc2019/'
watchAggregatedInputPath = "s3://hotstar-dp-datalake-processed-us-east-1-prod/aggregates/watched_video_daily_aggregates_ist_v4"
viewAggregatedInputPath = "s3://hotstar-dp-datalake-processed-us-east-1-prod/aggregates/viewed_page_daily_aggregates_ist_v2"
dau_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_full_v2/all/"
dau_prediction_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_predict/DAU_predict.parquet"

# spark.stop()
# spark = hive_spark('statistics')
# match_df = load_hive_table(spark, "in_cms.match_update_s3")
# save_data_frame(match_df, match_meta_path)

tournament_dic = {
    'sri_lanka_tour_of_india2023': {
        'tournament_name': 'Sri Lanka Tour of India 2023',
        'tournament_type': 'Tour',
        'match_type': '',
        'venue': 'India',
        'gender_type': 'men',
        'vod_type': 'svod',
        'match_num': 6
    },
    'new_zealand_tour_of_india2023': {
        'tournament_name': 'New Zealand Tour of India 2023',
        'tournament_type': 'Tour',
        'match_type': '',
        'venue': 'India',
        'gender_type': 'men',
        'vod_type': 'svod',
        'match_num': 6
    },
    'wc2022': {
        'tournament_name': 'World Cup 2022',
        'tournament_type': 'International',
        'match_type': 'T20',
        'venue': 'Australia',
        'gender_type': 'men',
        'vod_type': 'svod',
        'match_num': 45
    },
    'ac2022': {
        'tournament_name': 'Asia Cup 2022',
        'tournament_type': 'International',
        'match_type': 'T20',
        'venue': 'United Arab Emirates',
        'gender_type': 'men',
        'vod_type': 'svod',
        'match_num': 13
    },
    'south_africa_tour_of_india2022': {
        'tournament_name': 'South Africa Tour of India 2022',
        'tournament_type': 'Tour',
        'match_type': '',
        'venue': 'India',
        'gender_type': 'men',
        'vod_type': 'svod',
        'match_num': 11
    },
    'west_indies_tour_of_india2022': {
        'tournament_name': 'West Indies Tour of India 2022',
        'tournament_type': 'Tour',
        'match_type': '',
        'venue': 'India',
        'gender_type': 'men',
        'vod_type': 'svod',
        'match_num': 6
    },
    'ipl2022': {
        'tournament_name': 'IPL 2022',
        'tournament_type': 'National',
        'match_type': 'T20',
        'venue': 'India',
        'gender_type': 'men',
        'vod_type': 'svod',
        'match_num': 74
    },
    'england_tour_of_india2021': {
        'tournament_name': 'England Tour of India 2021',
        'tournament_type': 'Tour',
        'match_type': '',
        'venue': 'India',
        'gender_type': 'men',
        'vod_type': 'svod',
        'match_num': 28
    },
    'wc2021': {
        'tournament_name': 'World Cup 2021',
        'tournament_type': 'International',
        'match_type': 'T20',
        'venue': 'United Arab Emirates',
        'gender_type': 'men',
        'vod_type': 'svod',
        'match_num': 45
    },
    'ipl2021': {
        'tournament_name': 'IPL 2021',
        'tournament_type': 'National',
        'match_type': 'T20',
        'venue': 'India, United Arab Emirates',
        'gender_type': 'men',
        'vod_type': 'svod',
        'match_num': 62
    },
    'australia_tour_of_india2020': {
        'tournament_name': 'Australia Tour of India 2020',
        'tournament_type': 'Tour',
        'match_type': 'ODI',
        'venue': 'India',
        'gender_type': 'men',
        'vod_type': 'avod',
        'match_num': 3
    },
    'india_tour_of_new_zealand2020': {
        'tournament_name': 'India Tour of New Zealand 2020',
        'tournament_type': 'Tour',
        'match_type': '',
        'venue': 'New Zealand',
        'gender_type': 'men',
        'vod_type': 'avod',
        'match_num': 21
    },
    'ipl2020': {
        'tournament_name': 'IPL 2020',
        'tournament_type': 'National',
        'match_type': 'T20',
        'venue': 'India',
        'gender_type': 'men',
        'vod_type': 'svod',
        'match_num': 60
    },
    'west_indies_tour_of_india2019': {
        'tournament_name': 'West Indies Tour India 2019',
        'tournament_type': 'Tour',
        'match_type': '',
        'venue': 'India',
        'gender_type': 'men',
        'vod_type': 'avod',
        'match_num': 6
    },
    'wc2019': {
        'tournament_name': 'World Cup 2019',
        'tournament_type': 'International',
        'match_type': 'ODI',
        'venue': 'England',
        'gender_type': 'men',
        'vod_type': 'avod',
        'match_num': 48
    },
    'ipl2019': {
        'tournament_name': 'IPL 2019',
        'tournament_type': 'National',
        'match_type': 'T20',
        'venue': 'India',
        'gender_type': 'men',
        'vod_type': 'avod',
        'match_num': 60
    },
    'wc2023': {
        'tournament_name': 'World Cup 2023',
        'tournament_type': 'International',
        'match_type': 'ODI',
        'venue': 'India',
        'gender_type': 'men',
        'vod_type': 'avod',
        'match_num': 48
    },
    'ac2023': {
        'tournament_name': 'Asia Cup 2023',
        'tournament_type': 'International',
        'match_type': 'ODI',
        'venue': 'pakistan',
        'gender_type': 'men',
        'vod_type': 'avod',
        'match_num': 13
    }
}


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

# match_type: odi, t20, test
# tournament_name: IPL, World Cup
# tournament_type: international, national, tour
# match_stage: final, group, qualifier, semi-final
# ipl if_contain_india_team is true
# continent name
# ask a tier list from nirmal
# hotstar influence used as number
# calculate all sub number and free number
one_hot_cols = ['tournament_type', 'if_weekend', 'match_time', 'if_holiday', 'venue', 'if_contain_india_team',
                'match_type', 'tournament_name', 'hostar_influence',
                'match_stage', 'vod_type', 'gender_type']
multi_hot_cols = ['teams', 'continents', 'teams_tier']
additional_cols = ["languages", "platforms"]


def merge_sub_and_free_features(tournament):
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
        .withColumn('tournament', F.lit(tournament))\
        .where('date != "2022-08-24"')


def get_match_time(feature_df):
    # ipl2019_df = spark.createDataFrame([('ipl2019', '1440000316', '2019-05-08 17:30:00'), ('ipl2019', '1440000317', '2019-05-10 17:30:00')], ['tournament', 'content_id', 'start_time'])
    # save_data_frame(ipl2019_df, "s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/playout_v3/ipl2019")
    df = reduce(lambda x, y: x.union(y),
                [load_data_frame(spark, f"s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/playout_v3/{tournament}")
                .withColumn('tournament', F.lit(tournament)).select('tournament', 'content_id', 'start_time') for tournament in tournament_dic])\
        .cache()
    playout_df = df\
        .groupBy('tournament', 'content_id')\
        .agg(F.min('start_time').alias('match_start_time'))\
        .withColumn('match_start_time', F.from_unixtime(F.unix_timestamp(F.col('match_start_time'), 'yyyy-MM-dd HH:mm:ss') + F.lit(3620)))\
        .withColumn('match_start_time', F.substring(F.col('match_start_time'), 12, 8))\
        .cache()
    feature_df.groupBy('tournament').count().orderBy('tournament').show()
    res_df = feature_df\
        .join(playout_df, ['tournament', 'content_id'], 'left')
    res_df.where('match_start_time is null').groupBy('tournament').count().orderBy('tournament').show()
    res_df = res_df\
        .withColumn('rank', F.expr('row_number() over (partition by date order by content_id desc)'))\
        .withColumn('match_start_time', get_match_start_time_udf('content_id', 'rank', 'match_start_time', 'tournament')) \
        .withColumn('match_time', F.expr('if(match_start_time < "06:00:00", 0, '
                                         'if(match_start_time < "12:00:00", 1, '
                                         'if(match_start_time < "18:00:00", 2, 3)))')) \
        .orderBy('date', 'content_id')
    res_df.groupBy('tournament').count().orderBy('tournament').show()
    res_df.where('match_time is null').orderBy('date').show(200, False)
    res_df.show(200, False)
    res_df.where('tournament="wc2019"').orderBy('content_id').show(200, False)
    return res_df


def get_language_and_platform(tournament):
    if not check_s3_path_exist(live_ads_inventory_forecasting_root_path + f"/language_and_platform_of_{tournament}"):
        match_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"match_data/{tournament}").cache()
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
            .withColumn('platforms', F.array_distinct(F.col('platforms')))
        save_data_frame(watch_df, live_ads_inventory_forecasting_root_path + f"/language_and_platform_of_{tournament}")


def get_language_and_platform_all():
    for tournament in tournament_dic:
        print(tournament)
        get_language_and_platform(tournament)
    df = reduce(lambda x, y: x.union(y),
                          [load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/language_and_platform_of_{tournament}")
                           for tournament in tournament_dic]) \
        .withColumn('platforms', plaform_process_udf("platforms")) \
        .withColumn('languages', languages_process_udf("languages")) \
        .cache()
    df.select('languages').withColumn('language', F.explode('languages')).select('language').distinct().orderBy('language').show(200, False)
    df.select('platforms').withColumn('platform', F.explode('platforms')).select('platform').distinct().orderBy('platform').show(200, False)
    save_data_frame(df, live_ads_inventory_forecasting_root_path + f"/language_and_platform_all")


def add_more_features(feature_df):
    df = feature_df \
        .withColumn('tournament_type', F.expr('if(locate("ipl", tournament) > 0, "national", if(locate("tour", tournament) > 0, "tour", "international"))'))\
        .withColumn('if_holiday', if_holiday_udf('date'))\
        .withColumn('venue_detail', get_venue_udf('tournament', 'date'))\
        .withColumn('venue', F.expr('if(venue_detail="india", 1, 0)'))\
        .withColumn('match_type', get_match_type_udf('tournament', 'date'))\
        .withColumn('tournament_name', get_tournament_name_udf('tournament'))\
        .withColumn('year', get_year_udf('tournament'))\
        .withColumn('hostar_influence', F.expr('year - 2016'))\
        .withColumn('rank', F.expr('row_number() over (partition by tournament order by date)'))\
        .withColumn('match_stage', get_match_stage_udf('tournament', 'date', 'rank')) \
        .withColumn('vod_type', F.expr('if(date <= "2020-08-30" or date >= "2023-09-01", "avod", "svod")')) \
        .withColumn('gender_type', get_gender_type_udf('tournament')) \
        .withColumn('teams', simple_title_udf('title'))\
        .withColumn('continents', get_continents_udf('teams', 'tournament_type'))\
        .withColumn('teams_tier', get_teams_tier_udf('teams'))\
        .cache()
    df.groupBy('tournament', 'tournament_type')\
        .agg(F.countDistinct('match_type'), F.collect_set('match_type'),
             F.countDistinct('match_stage'), F.collect_set('match_stage'))\
        .orderBy('tournament_type')\
        .show(20, False)
    df.where('continents = "" or teams_tier = ""').select('tournament', 'title', 'teams', 'continents', 'teams_tier').show(20, False)
    return df


def add_hots_features(feature_df, type="train", if_language_and_platform=True, root_path=live_ads_inventory_forecasting_root_path):
    col_num_dic = {}
    df = feature_df\
        .withColumn('rank', F.expr('row_number() over (partition by tournament order by date)'))\
        .cache()
    df.groupBy('tournament').count().orderBy('tournament').show()
    for col in multi_hot_cols:
        print(col)
        df2 = df\
            .select('tournament', 'rank', f"{col}")\
            .withColumn(f"{col}_list", F.split(F.col(col), " vs "))\
            .withColumn(f"{col}_item", F.explode(F.col(f"{col}_list")))
        if type == "train":
            col_df = df2 \
                .select(f"{col}_item") \
                .distinct() \
                .withColumn('tag', F.lit(1)) \
                .withColumn(f"{col}_hots", F.expr(f'row_number() over (partition by tag order by {col}_item)')) \
                .withColumn(f"{col}_hots", F.expr(f'{col}_hots - 1')) \
                .drop('tag') \
                .cache()
            save_data_frame(col_df, live_ads_inventory_forecasting_root_path + "/feature_mapping/" + col)
        col_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + "/feature_mapping/" + col).cache()
        col_num = col_df.count()
        print(col_num)
        col_num_dic[col] = col_num
        print(df2.select('tournament', 'rank', f"{col}_item").join(col_df, f"{col}_item", 'left').where(f"{col}_hots is null").count())
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
    save_data_frame(df, root_path + "/baseline_features_with_all_features_multi_hots")
    df = load_data_frame(spark, root_path + "/baseline_features_with_all_features_multi_hots")
    for col in one_hot_cols:
        print(col)
        if type == "train":
            col_df = df\
                .select(col)\
                .distinct()\
                .withColumn('tag', F.lit(1))\
                .withColumn(f"{col}_hots", F.expr(f'row_number() over (partition by tag order by {col})'))\
                .withColumn(f"{col}_hots", F.expr(f'{col}_hots - 1'))\
                .drop('tag')\
                .cache()
            save_data_frame(col_df, live_ads_inventory_forecasting_root_path + "/feature_mapping/" + col)
        col_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + "/feature_mapping/" + col).cache()
        col_num = col_df.count()
        col_df = col_df.withColumn(f"{col}_hots", F.array(F.col(f"{col}_hots")))
        print(col_num)
        col_num_dic[col] = col_num
        print(df.join(col_df, col, 'left').where(f"{col}_hots is null").count())
        df = df\
            .join(col_df, col)\
            .withColumn(f"{col}_hots_num", F.lit(col_num))
    for col in one_hot_cols:
        print(col)
        df = df\
            .withColumn(f"{col}_hot_vector", generate_hot_vector_udf(f"{col}_hots", f"{col}_hots_num"))
    for col in ['hostar_influence']:
        print(col)
        df = df.withColumn(f"{col}_hot_vector", F.array(F.col(f"{col}")))
    df.groupBy('tournament').count().orderBy('tournament').show()
    save_data_frame(df, root_path + "/baseline_features_with_all_features_hots")
    df = load_data_frame(spark, root_path + "/baseline_features_with_all_features_hots")\
        .cache()
    df.groupBy('tournament').count().orderBy('tournament').show()
    if if_language_and_platform:
        language_and_platform_df = load_data_frame(spark, root_path + f"/language_and_platform_all")\
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
        # cols = [col+"_hot_vector" for col in one_hot_cols]
        # df.where('date = "2022-11-13"').select('content_id', *cols).show(20, False)
        # cols = [col+"_hot_vector" for col in multi_hot_cols]
        # df.where('date = "2022-11-13"').select('content_id', *cols).show(20, False)
        # cols = [col+"_hot_vector" for col in additional_cols]
        # df.where('date = "2022-11-13"').select('content_id', *cols).show(20, False)
        print(col_num_dic)
    return df


def add_free_timer_features(feature_df):
    avod_tournament_list = ['wc2019', 'west_indies_tour_of_india2019', 'australia_tour_of_india2020', 'india_tour_of_new_zealand2020']
    #  'content_id', 'carrier_jio', 'match_active_free_num', 'total_free_watch_time', 'avg_watch_time', 'date'
    free_timer_df = reduce(lambda x, y: x.union(y),
                           [load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/free_watch_time_detailed_of_{tournament}").withColumn('tournament', F.lit(tournament))
                            for tournament in avod_tournament_list])\
        .withColumn('free_timer', F.expr('if(carrier_jio=1, 1000, 10)'))\
        .cache()
    free_timer_df = free_timer_df\
        .join(free_timer_df.groupBy('content_id').agg(F.sum('match_active_free_num').alias('total_match_active_free_num')), 'content_id')\
        .withColumn('user_rate', F.expr('match_active_free_num/total_match_active_free_num'))\
        .cache()
    free_timer_df.orderBy('date').show(500, False)
    res_df = feature_df\
        .join(free_timer_df.selectExpr('date', 'content_id', 'user_rate', 'free_timer', 'avg_watch_time as watch_time_per_free_per_match_with_free_timer'),
              ['date', 'content_id'], 'left')\
        .fillna(5, ['free_timer'])\
        .fillna(1.0, ['user_rate'])\
        .fillna(-1, ['watch_time_per_free_per_match_with_free_timer'])\
        .withColumn('watch_time_per_free_per_match_with_free_timer', F.expr('if(watch_time_per_free_per_match_with_free_timer<0, watch_time_per_free_per_match, watch_time_per_free_per_match_with_free_timer)'))
    return res_df


def generate_prediction_dataset(tournament):
    file_name = f"{tournament}.csv"
    # tournament = "wc2023"
    parameter_list = ['total_frees_number', 'active_free_num', 'match_active_free_num', 'total_free_watch_time',
                        'total_subscribers_number', 'active_sub_num', 'match_active_sub_num', 'total_sub_watch_time',
                        'active_frees_rate', 'frees_watching_match_rate', 'watch_time_per_free_per_match',
                        'active_subscribers_rate', 'subscribers_watching_match_rate', 'watch_time_per_subscriber_per_match']
    feature_df = load_data_frame(spark, file_name, "csv", True) \
        .withColumn("if_contain_india_team", F.locate('india', F.col('title'))) \
        .withColumn('if_contain_india_team', F.expr('if(if_contain_india_team > 0, 1, 0)')) \
        .withColumn('if_weekend', F.dayofweek(F.col('date'))) \
        .withColumn('if_weekend', F.expr('if(if_weekend=1 or if_weekend = 7, 1, 0)'))\
        .withColumn('tournament', F.lit(tournament))\
        .withColumn('shortsummary', F.lit(tournament))\
        .withColumn('content_id', F.expr('cast(cast(100000+rank as int) as string)'))\
        .withColumn('content_id', F.concat(F.col('tournament'), F.col('content_id')))\
        .withColumn('rank', F.expr('row_number() over (partition by date order by content_id desc)'))\
        .withColumn('match_start_time', F.expr('if(rank=1, "14:00:00", "09:00:00")')) \
        .withColumn('match_time', F.expr('if(match_start_time < "06:00:00", 0, '
                                         'if(match_start_time < "12:00:00", 1, '
                                         'if(match_start_time < "18:00:00", 2, 3)))'))
    res_df = add_more_features(feature_df)
    save_data_frame(res_df, live_ads_inventory_forecasting_complete_feature_path + "/" + tournament + "all_features")
    feature_df = load_data_frame(spark, live_ads_inventory_forecasting_complete_feature_path + "/" + tournament + "all_features")
    res_df = add_hots_features(feature_df, type="test", if_language_and_platform=False, root_path=live_ads_inventory_forecasting_complete_feature_path+"/"+tournament)
    res_df.orderBy('date', 'content_id').show(50, False)
    for parameter in parameter_list:
        res_df = res_df.withColumn(parameter, F.lit(-1.0))
    base_path_suffix = "/all_features_hots_format"
    save_data_frame(res_df, live_ads_inventory_forecasting_complete_feature_path+"/"+tournament+"/"+base_path_suffix)
    for path_suffix in ["/"+tournament+base_path_suffix]:
        feature_df = load_data_frame(spark, live_ads_inventory_forecasting_complete_feature_path + path_suffix).cache()
        for col in one_hot_cols:
            if feature_df.select(f"{col}_hots_num").distinct().collect()[0][0] == 2:
                print(col)
                feature_df = feature_df\
                    .withColumn(f"{col}_hots_num", F.lit(1))\
                    .withColumn(f"{col}_hot_vector", F.col(f"{col}_hots"))
        save_data_frame(feature_df, live_ads_inventory_forecasting_complete_feature_path + path_suffix + "_and_simple_one_hot")
    cols = [col+"_hot_vector" for col in one_hot_cols+multi_hot_cols+additional_cols]
    df = load_data_frame(spark, live_ads_inventory_forecasting_complete_feature_path + path_suffix + "_and_simple_one_hot")\
        .drop(*cols)
    df.orderBy('date', 'content_id').show(3000, False)


generate_prediction_dataset("ac2023")


# feature_df = reduce(lambda x, y: x.union(y), [merge_sub_and_free_features(tournament) for tournament in tournament_dic])
# path_suffix = "/wt_features"
# save_data_frame(feature_df, live_ads_inventory_forecasting_complete_feature_path + path_suffix)
#
# feature_df = load_data_frame(spark, live_ads_inventory_forecasting_complete_feature_path + path_suffix).cache()
# path_suffix = "/wt_features_with_match_start_time"
# res_df = get_match_time(feature_df)
# save_data_frame(res_df, live_ads_inventory_forecasting_complete_feature_path + path_suffix)
#
# get_language_and_platform_all()
#
#
# feature_df = load_data_frame(spark, live_ads_inventory_forecasting_complete_feature_path + path_suffix).cache()
# path_suffix = "/all_features"
# res_df = add_more_features(feature_df)
# save_data_frame(res_df, live_ads_inventory_forecasting_complete_feature_path + path_suffix)
#

# feature_df = load_data_frame(spark, live_ads_inventory_forecasting_complete_feature_path + path_suffix).cache()
# path_suffix = "/all_features_hots_format"
# res_df = add_hots_features(feature_df)
# save_data_frame(res_df, live_ads_inventory_forecasting_complete_feature_path + path_suffix)


# feature_df = load_data_frame(spark, live_ads_inventory_forecasting_complete_feature_path + path_suffix)\
#     .drop('total_frees_number', 'active_frees_rate')\
#     .withColumn("rand", F.rand(seed=4321))\
#     .cache()
# print(feature_df.count())
# sub_and_free_num_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/sub_num")\
#     .join(load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/user_num"), 'cd')\
#     .withColumn('total_frees_number', F.expr('user_num - sub_num'))\
#     .cache()
# # sub_and_free_num_df.orderBy('cd').show(1000)
# path_suffix = "/all_features_hots_format_with_new_sub_free_num"
# res_df = feature_df\
#     .join(sub_and_free_num_df.selectExpr('cd as date', 'total_frees_number'), 'date')\
#     .withColumn('active_frees_rate', F.expr('active_free_num/total_frees_number'))
# print(res_df.count())
# save_data_frame(res_df, live_ads_inventory_forecasting_complete_feature_path + path_suffix)
#
# feature_df = load_data_frame(spark, live_ads_inventory_forecasting_complete_feature_path + path_suffix).cache()
# path_suffix = "/all_features_hots_format_with_new_sub_free_num_and_free_timer"
# res_df = add_free_timer_features(feature_df)
# save_data_frame(res_df, live_ads_inventory_forecasting_complete_feature_path + path_suffix)
#
#
# for path_suffix in ["/all_features_hots_format_with_new_sub_free_num", "/all_features_hots_format_with_new_sub_free_num_and_free_timer"]:
#     feature_df = load_data_frame(spark, live_ads_inventory_forecasting_complete_feature_path + path_suffix).cache()
#     for col in one_hot_cols:
#         if feature_df.select(f"{col}_hots_num").distinct().collect()[0][0] == 2:
#             print(col)
#             feature_df = feature_df\
#                 .withColumn(f"{col}_hots_num", F.lit(1))\
#                 .withColumn(f"{col}_hot_vector", F.col(f"{col}_hots"))
#     save_data_frame(feature_df, live_ads_inventory_forecasting_complete_feature_path + path_suffix + "_and_simple_one_hot")
#


# feature_df = load_data_frame(spark, live_ads_inventory_forecasting_complete_feature_path + path_suffix).cache()


path_suffix = "/all_features_hots_format"
# res_df = add_hots_features(feature_df)
# save_data_frame(res_df, live_ads_inventory_forecasting_complete_feature_path + path_suffix)
#
#
feature_df = load_data_frame(spark, live_ads_inventory_forecasting_complete_feature_path + path_suffix)\
    .where('content_id not in ("1440000689", "1440000694", "1440000696", "1440000982", '
           '"1540019005", "1540019014", "1540019017","1540016333")')\
    .drop('total_frees_number', 'frees_watching_match_rate', 'total_subscribers_number', 'subscribers_watching_match_rate')\
    .withColumn("rand", F.rand(seed=54321))\
    .cache()
print(feature_df.count())

# match_num_df = feature_df\
#     .groupBy('date')\
#     .agg(F.count('content_id').alias('match_num'))\
#     .withColumn('match_num_hots_num', F.lit(3))\
#     .withColumn('match_num', F.expr('match_num - 1'))\
#     .withColumn('match_num_hots', F.array(F.col('match_num')))\
#     .withColumn(f"match_num_hot_vector", generate_hot_vector_udf(f"match_num_hots", f"match_num_hots_num"))\
#     .cache()
# match_num_df.show()
# save_data_frame(match_num_df, live_ads_inventory_forecasting_complete_feature_path + "/match_num")

# using predicted dau as label
# sub_and_free_num_df = load_data_frame(spark, dau_prediction_path)\
#     .withColumn('total_frees_number', F.expr('DAU - subs_DAU'))\
#     .selectExpr('cd as date', 'total_frees_number', 'subs_DAU as total_subscribers_number')\
#     .join(feature_df.select('date', 'tournament').distinct(), 'date')\
#     .groupBy('tournament')\
#     .agg(F.avg('total_frees_number').alias('total_frees_number'),
#          F.avg('total_subscribers_number').alias('total_subscribers_number'))\
#     .cache()
# path_suffix = "/all_features_hots_format_with_avg_predicted_au_sub_free_num"


# # using gt dau as label
# sub_and_free_num_df = load_data_frame(spark, dau_path)\
#     .withColumn('total_frees_number', F.expr('vv - sub_vv'))\
#     .selectExpr('ds as date', 'total_frees_number', 'sub_vv as total_subscribers_number')\
#     .join(feature_df.select('date', 'tournament').distinct(), 'date')\
#     .groupBy('tournament')\
#     .agg(F.avg('total_frees_number').alias('total_frees_number'),
#          F.avg('total_subscribers_number').alias('total_subscribers_number'))\
#     .cache()
# path_suffix = "/all_features_hots_format_with_avg_au_sub_free_num"
# # using gt dau as label
#
# res_df = feature_df\
#     .join(sub_and_free_num_df, 'tournament')\
#     .withColumn('frees_watching_match_rate', F.expr('match_active_free_num/total_frees_number'))\
#     .withColumn('subscribers_watching_match_rate', F.expr('match_active_sub_num/total_subscribers_number'))
# print(res_df.count())
# save_data_frame(res_df, live_ads_inventory_forecasting_complete_feature_path + path_suffix)
#
# feature_df = load_data_frame(spark, live_ads_inventory_forecasting_complete_feature_path + path_suffix).cache()
# path_suffix += "_and_free_timer"
# res_df = add_free_timer_features(feature_df)
# save_data_frame(res_df, live_ads_inventory_forecasting_complete_feature_path + path_suffix)
#
#
# for path_suffix_tmp in [path_suffix.replace("_and_free_timer", ""), path_suffix]:
#     feature_df = load_data_frame(spark, live_ads_inventory_forecasting_complete_feature_path + path_suffix_tmp).cache()
#     for col in one_hot_cols:
#         if feature_df.select(f"{col}_hots_num").distinct().collect()[0][0] == 2:
#             print(col)
#             feature_df = feature_df\
#                 .withColumn(f"{col}_hots_num", F.lit(1))\
#                 .withColumn(f"{col}_hot_vector", F.col(f"{col}_hots"))
#     save_data_frame(feature_df, live_ads_inventory_forecasting_complete_feature_path + path_suffix_tmp + "_and_simple_one_hot")
#
#
# cols = [col+"_hot_vector" for col in one_hot_cols+multi_hot_cols+additional_cols]
# df = load_data_frame(spark, live_ads_inventory_forecasting_complete_feature_path + path_suffix.replace("_and_free_timer", ""))\
#     .drop(*cols)
# df.orderBy('date', 'content_id').select('date', 'content_id', 'tournament', 'title', 'total_frees_number', 'frees_watching_match_rate', 'total_subscribers_number', 'subscribers_watching_match_rate').show(3000, False)


# only use jio users for free rate label
path_suffix = "/all_features_hots_format_with_avg_au_sub_free_num"
free_dau_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/free_dau")\
    .withColumnRenamed('cd', 'date')\
    .where('carrier_jio=1 and date >= "2019-05-30"')\
    .select('date', 'active_free_num')\
    .withColumnRenamed('active_free_num', 'total_frees_number')\
    .cache()
feature_df = load_data_frame(spark, live_ads_inventory_forecasting_complete_feature_path + path_suffix).cache()
feature_df.groupBy('tournament').count().show()
free_num_df = free_dau_df\
    .join(feature_df.select('date', 'tournament').distinct(), 'date')\
    .groupBy('tournament')\
    .agg(F.avg('total_frees_number').alias('new_total_frees_number'))\
    .cache()
free_num_df.show()
jio_reach_df = load_data_frame(spark, live_ads_inventory_forecasting_complete_feature_path + path_suffix + "_and_free_timer")\
    .where('tournament="wc2019" and free_timer = 1000')\
    .withColumn('new_match_active_free_num', F.expr('match_active_free_num * user_rate'))\
    .select('content_id', 'new_match_active_free_num', 'free_timer')\
    .cache()
print(jio_reach_df.count())
jio_reach_df.show()
res_df = feature_df\
    .join(free_num_df, 'tournament', 'left')\
    .join(jio_reach_df, 'content_id', 'left') \
    .withColumn('total_frees_number', F.expr('if(tournament="wc2019", new_total_frees_number, total_frees_number)'))\
    .withColumn('match_active_free_num', F.expr('if(tournament="wc2019", new_match_active_free_num, match_active_free_num)'))\
    .withColumn('frees_watching_match_rate', F.expr('match_active_free_num/total_frees_number'))\
    .drop('new_total_frees_number', 'new_match_active_free_num')
print(feature_df.count())
print(res_df.count())
# res_df.where('tournament="wc2019" or tournament="wc2022"')\
#     .select('date', 'total_frees_number', 'new_total_frees_number',
#             'match_active_free_num', 'new_match_active_free_num',
#             'frees_watching_match_rate')\
#     .show(50)
path_suffix += "_full_avod_2019"
save_data_frame(res_df, live_ads_inventory_forecasting_complete_feature_path + path_suffix)


for path_suffix_tmp in [path_suffix]:
    feature_df = load_data_frame(spark, live_ads_inventory_forecasting_complete_feature_path + path_suffix_tmp).cache()
    for col in one_hot_cols:
        if feature_df.select(f"{col}_hots_num").distinct().collect()[0][0] == 2:
            print(col)
            feature_df = feature_df\
                .withColumn(f"{col}_hots_num", F.lit(1))\
                .withColumn(f"{col}_hot_vector", F.col(f"{col}_hots"))
    save_data_frame(feature_df, live_ads_inventory_forecasting_complete_feature_path + path_suffix_tmp + "_and_simple_one_hot")



# save_data_frame(df, live_ads_inventory_forecasting_root_path + f"/baseline_features_final/all_features_hots_format_csv", "csv", True, '###')
# feature_df.groupBy('tournament').count().show()
# feature_df\
#     .withColumn('tag', F.locate('ipl', F.col('tournament')))\
#     .where('tag > 0')\
#     .withColumn('simple_title', simple_title_udf('title'))\
#     .withColumn('teams', F.split(F.col('simple_title'), ' vs '))\
#     .withColumn('team', F.explode('teams'))\
#     .select('team').distinct().orderBy('team').show(3000, False)


