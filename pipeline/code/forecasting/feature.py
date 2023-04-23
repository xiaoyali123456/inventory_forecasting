import datetime
import os
import sys
import time
import pickle
from functools import reduce
from math import log
import itertools
import math
import holidays
import s3fs
import json
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.shell import spark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
from common import load_requests

storageLevel = StorageLevel.DISK_ONLY
distribution_fun = "default"
# distribution_fun = "gaussian"
valid_creative_dic = {}


def check_s3_path_exist(s3_path: str) -> bool:
    if not s3_path.endswith("/"):
        s3_path += "/"
    return os.system(f"aws s3 ls {s3_path}_SUCCESS") == 0


def check_s3_path_exist_simple(s3_path: str) -> bool:
    if not s3_path.endswith("/"):
        s3_path += "/"
    return os.system(f"aws s3 ls {s3_path}") == 0


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
    else:
        return "group"


def generate_hot_vector(hots, hots_num):
    res = [0 for i in range(hots_num)]
    for hot in hots:
        res[hot] += 1
    return res


def add_hots_features(feature_df, type="train", root_path=""):
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
        # print(col_num)
        col_num_dic[col] = col_num
        unvalid_num = df2.select('tournament', 'rank', f"{col}_item").join(col_df, f"{col}_item", 'left').where(f"{col}_hots is null").count()
        if unvalid_num > 0:
            print(unvalid_num)
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
        # print(col_num)
        col_num_dic[col] = col_num
        unvalid_num = df.join(col_df, col, 'left').where(f"{col}_hots is null").count()
        if unvalid_num > 0:
            print(unvalid_num)
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
    return df


def main(spark, date, content_id, tournament_name, match_type,
         venue, match_stage, gender_type, vod_type, match_start_time_ist):
    spark.stop()
    # os.system('pip install --upgrade holidays')
    spark = hive_spark('statistics')
    match_df = load_hive_table(spark, "in_cms.match_update_s3")\
        .where(f'content_id = "{content_id}"')\
        .selectExpr('contentid as content_id', 'title', 'shortsummary')\
        .distinct()\
        .cache()
    active_user_df = load_data_frame(spark, f'{active_user_num_path}/cd={date}')\
        .withColumn('total_frees_number', F.expr('vv - sub_vv'))\
        .withColumn('content_id', F.lit(content_id))\
        .selectExpr('ds as date', 'content_id', 'total_frees_number', 'sub_vv as total_subscribers_number')\
        .cache()
    match_sub_df = load_data_frame(spark, f'{watchAggregatedInputPath}/cd={date}', fmt="orc")\
        .withColumn('subscription_status', F.upper(F.col('subscription_status')))\
        .where(f'content_id = "{content_id}" and subscription_status in ("ACTIVE", "CANCELLED", "GRACEPERIOD")')\
        .groupBy('dw_p_id', 'content_id')\
        .agg(F.sum('watch_time').alias('watch_time')) \
        .groupBy('content_id') \
        .agg(F.countDistinct('dw_p_id').alias('match_active_sub_num'),
             F.sum('watch_time').alias('total_watch_time')) \
        .withColumn('watch_time_per_subscriber_per_match', F.expr('total_watch_time/match_active_sub_num'))\
        .select('content_id', 'match_active_sub_num', 'watch_time_per_subscriber_per_match')\
        .cache()
    match_free_df = load_data_frame(spark, f'{watchAggregatedInputPath}/cd={date}', fmt="orc")\
        .withColumn('subscription_status', F.upper(F.col('subscription_status')))\
        .where(f'content_id = "{content_id}" and subscription_status not in ("ACTIVE", "CANCELLED", "GRACEPERIOD")')\
        .groupBy('dw_p_id', 'content_id')\
        .agg(F.sum('watch_time').alias('watch_time')) \
        .groupBy('content_id') \
        .agg(F.countDistinct('dw_p_id').alias('match_active_free_num'),
             F.sum('watch_time').alias('total_free_watch_time')) \
        .withColumn('watch_time_per_free_per_match', F.expr('total_free_watch_time/match_active_free_num')) \
        .select('content_id', 'match_active_free_num', 'watch_time_per_free_per_match') \
        .cache()
    holiday = 1 if date in holidays.country_holidays('IN') else 0
    feature_df = match_df\
        .join(active_user_df, 'content_id')\
        .join(match_sub_df, 'content_id')\
        .join(match_free_df, 'content_id') \
        .withColumn('active_frees_rate', F.lit(-1.0)) \
        .withColumn('active_subscribers_rate', F.lit(-1.0)) \
        .withColumn('frees_watching_match_rate', F.expr('match_active_free_num/total_frees_number')) \
        .withColumn('subscribers_watching_match_rate', F.expr('match_active_sub_num/total_subscribers_number'))\
        .withColumn("if_contain_india_team", F.locate('india', F.col('title'))) \
        .withColumn('if_contain_india_team', F.expr('if(if_contain_india_team > 0, 1, 0)')) \
        .withColumn('if_weekend', F.dayofweek(F.col('date'))) \
        .withColumn('if_weekend', F.expr('if(if_weekend=1 or if_weekend = 7, 1, 0)')) \
        .withColumn('tournament', F.lit(tournament_name.replace(" ", "_").lower())) \
        .withColumn('match_start_time', F.lit(match_start_time_ist)) \
        .withColumn('match_time', F.expr('if(match_start_time < "06:00:00", 0, '
                                         'if(match_start_time < "12:00:00", 1, '
                                         'if(match_start_time < "18:00:00", 2, 3)))')) \
        .withColumn('tournament_type', F.expr('if(locate("ipl", tournament) > 0, "national", if(locate("tour", tournament) > 0, "tour", "international"))')) \
        .withColumn('if_holiday', F.lit(holiday)) \
        .withColumn('venue_detail', F.lit(venue.lower())) \
        .withColumn('venue', F.expr('if(venue_detail="india", 1, 0)')) \
        .withColumn('match_type', F.lit(match_type.lower())) \
        .withColumn('tournament_name', F.lit(" ".join(tournament_name.lower().split(" ")[:-1]))) \
        .withColumn('year', F.lit(int(tournament_name.lower().split(" ")[-1]))) \
        .withColumn('hostar_influence', F.expr('year - 2016')) \
        .withColumn('match_stage', F.lit(match_stage.lower())) \
        .withColumn('vod_type', F.lit(vod_type.lower())) \
        .withColumn('gender_type', F.lit(gender_type.lower())) \
        .withColumn('teams', simple_title_udf('title')) \
        .withColumn('continents', get_continents_udf('teams', 'tournament_type')) \
        .withColumn('teams_tier', get_teams_tier_udf('teams')) \
        .withColumn('free_timer', F.expr('if(vod_type="avod", 1000, 5)')) \
        .cache()
    feature_df = add_hots_features(feature_df, type="test", root_path=pipeline_base_path + f"/dataset")
    base_path_suffix = "/all_features_hots_format"
    # save_data_frame(feature_df, pipeline_base_path + base_path_suffix + f"/cd={date}/contentid={content_id}")
    for col in one_hot_cols:
        if feature_df.select(f"{col}_hots_num").distinct().collect()[0][0] == 2:
            print(col)
            feature_df = feature_df\
                .withColumn(f"{col}_hots_num", F.lit(1))\
                .withColumn(f"{col}_hot_vector", F.col(f"{col}_hots"))
    save_data_frame(feature_df, pipeline_base_path + base_path_suffix + "_and_simple_one_hot" + f"/cd={date}/contentid={content_id}")
    save_data_frame(feature_df, pipeline_base_path + base_path_suffix + "_and_free_timer_and_simple_one_hot" + f"/cd={date}/contentid={content_id}")
    cols = [col+"_hot_vector" for col in one_hot_cols+multi_hot_cols+additional_cols]
    df = load_data_frame(spark, pipeline_base_path + base_path_suffix + "_and_simple_one_hot" + f"/cd={date}/contentid={content_id}")\
        .drop(*cols)
    df.orderBy('date', 'content_id').show(3000, False)
    return feature_df


def save_base_dataset(path_suffix):
    df = load_data_frame(spark, live_ads_inventory_forecasting_complete_feature_path + "/all_features_hots_format_with_avg_au_sub_free_num" + path_suffix)\
        .cache()
    base_path_suffix = "/all_features_hots_format"
    contents = df.select('date', 'content_id').distinct().collect()
    for content in contents:
        print(content)
        date = content[0]
        content_id = content[1]
        save_data_frame(df.where(f'date="{date}" and content_id="{content_id}"'), pipeline_base_path + base_path_suffix + path_suffix + f"/cd={date}/contentid={content_id}")


def generate_prediction_dataset(today, config):
    res = []
    cols = ['request_id', 'tournament_name', 'tournament', 'match_type', 'vod_type', 'venue_detail', 'free_timer',
            'content_id', 'match_stage', 'date', 'year', 'match_start_time', 'title', 'if_holiday', 'match_duration', 'break_duration']
    for request in config:
        request_id = request['id']
        tournament_name = request['tournamentName'].lower()
        tournament = request['seasonName'].replace(" ", "_").lower()
        vod_type = request['tournamentType'].lower()
        venue_detail = request['tournamentLocation'].lower()
        free_timer = request['svodFreeTimeDuration']
        for match in request['matchDetails']:
            content_id = f"{request_id}-{match['matchId']}"
            date = match['matchDate']
            year = int(date[:4])
            match_start_time = f"{match['matchStartHour']}:00:00"
            if len(match_start_time) < 8:
                match_start_time = "0" + match_start_time
            match_stage = match['matchType']
            title = match['teams'][0]['name'].lower() + " vs " + match['teams'][1]['name'].lower()
            # tiers = match['teams'][0]['tier'].lower() + " vs " + match['teams'][1]['tier'].lower()
            if_holiday = match['publicHoliday']
            match_type = match['tournamentCategory'].lower()
            match_duration = int(match['estimatedMatchDuration'])
            break_duration = float(match['fixedBreak'] * match['averageBreakDuration'] + match['adhocBreak'] * match['adhocBreakDuration'])
            res.append((request_id, tournament_name, tournament, match_type, vod_type, venue_detail, free_timer,
                        content_id, match_stage, date, year, match_start_time, title, if_holiday, match_duration, break_duration))
    prediction_df = spark.createDataFrame(res, cols) \
        .withColumn('total_frees_number', F.lit(-1)) \
        .withColumn('total_subscribers_number', F.lit(-1)) \
        .withColumn('active_frees_rate', F.lit(-1.0)) \
        .withColumn('active_subscribers_rate', F.lit(-1.0)) \
        .withColumn('frees_watching_match_rate', F.lit(-1.0)) \
        .withColumn('subscribers_watching_match_rate', F.lit(-1.0)) \
        .withColumn('watch_time_per_free_per_match', F.lit(-1.0)) \
        .withColumn('watch_time_per_subscriber_per_match', F.lit(-1.0)) \
        .withColumn('gender_type', F.lit("men")) \
        .withColumn("if_contain_india_team", F.locate('india', F.col('title'))) \
        .withColumn('if_contain_india_team', F.expr('if(if_contain_india_team > 0, 1, 0)')) \
        .withColumn('if_weekend', F.dayofweek(F.col('date'))) \
        .withColumn('if_weekend', F.expr('if(if_weekend=1 or if_weekend = 7, 1, 0)')) \
        .withColumn('match_time', F.expr('if(match_start_time < "06:00:00", 0, '
                                         'if(match_start_time < "12:00:00", 1, '
                                         'if(match_start_time < "18:00:00", 2, 3)))')) \
        .withColumn('tournament_type', F.expr('if(locate("ipl", tournament) > 0, "national", if(locate("tour", tournament) > 0, "tour", "international"))')) \
        .withColumn('if_holiday', F.expr('if(if_holiday="true", 1, 0)')) \
        .withColumn('venue', F.expr('if(venue_detail="india", 1, 0)')) \
        .withColumn('hostar_influence', F.expr('year - 2016')) \
        .withColumn('teams', simple_title_udf('title')) \
        .withColumn('continents', get_continents_udf('teams', 'tournament_type')) \
        .withColumn('teams_tier', get_teams_tier_udf('teams')) \
        .withColumn('free_timer', F.expr('if(vod_type="avod", 1000, free_timer)')) \
        .cache()
    feature_df = add_hots_features(prediction_df, type="test", root_path=pipeline_base_path + f"/dataset")
    base_path_suffix = "/prediction/all_features_hots_format"
    save_data_frame(feature_df, pipeline_base_path + base_path_suffix + f"/cd={today}")
    for col in one_hot_cols:
        if feature_df.select(f"{col}_hots_num").distinct().collect()[0][0] == 2:
            print(col)
            feature_df = feature_df \
                .withColumn(f"{col}_hots_num", F.lit(1)) \
                .withColumn(f"{col}_hot_vector", F.col(f"{col}_hots"))
    save_data_frame(feature_df.drop('free_timer'), pipeline_base_path + base_path_suffix + "_and_simple_one_hot" + f"/cd={today}")
    save_data_frame(feature_df, pipeline_base_path + base_path_suffix + "_and_free_timer_and_simple_one_hot" + f"/cd={today}")
    cols = [col + "_hot_vector" for col in one_hot_cols + multi_hot_cols + additional_cols]
    df = load_data_frame(spark,
                         pipeline_base_path + base_path_suffix + "_and_simple_one_hot" + f"/cd={today}") \
        .drop(*cols)
    df.orderBy('date', 'content_id').show(3000, False)
    return feature_df


check_title_valid_udf = F.udf(check_title_valid, IntegerType())
get_match_start_time_udf = F.udf(get_match_start_time, StringType())
strip_udf = F.udf(lambda x: x.strip(), StringType())
simple_title_udf = F.udf(simple_title, StringType())
get_teams_tier_udf = F.udf(get_teams_tier, StringType())
get_continents_udf = F.udf(get_continents, StringType())
get_match_stage_udf = F.udf(get_match_stage, StringType())
generate_hot_vector_udf = F.udf(generate_hot_vector, ArrayType(IntegerType()))

play_out_log_input_path = "s3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test"
watchAggregatedInputPath = "s3://hotstar-dp-datalake-processed-us-east-1-prod/aggregates/watched_video_daily_aggregates_ist_v4"
active_user_num_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_v3/truth/"
live_ads_inventory_forecasting_root_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting"
live_ads_inventory_forecasting_complete_feature_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/complete_features"
pipeline_base_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/pipeline"
dau_prediction_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_v3/forecast/"


one_hot_cols = ['tournament_type', 'if_weekend', 'match_time', 'if_holiday', 'venue', 'if_contain_india_team',
                'match_type', 'tournament_name', 'hostar_influence',
                'match_stage', 'vod_type', 'gender_type']
multi_hot_cols = ['teams', 'continents', 'teams_tier']
additional_cols = ["languages", "platforms"]

# save_base_dataset("")
# save_base_dataset("_and_simple_one_hot")
# save_base_dataset("_and_free_timer")
# save_base_dataset("_and_free_timer_and_simple_one_hot")
# main(spark, date, content_id, tournament_name, match_type, venue, match_stage, gender_type, vod_type, match_start_time_ist)

# print("argv", sys.argv)

DATE=sys.argv[1]
config = load_requests(DATE)
generate_prediction_dataset(DATE, config=config)
