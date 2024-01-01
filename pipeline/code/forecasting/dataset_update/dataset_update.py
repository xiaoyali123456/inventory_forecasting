"""
1. load parsed request data from s3
2. feature process for ML models
3. calculate avg sub/free DAU at tournament level from the combination data of gt DAU and predicted DAU
4. generate prediction dataset for future matches
5. add finished focal matches to old training dataset
    5.1 calculate sub/free vv and avg_wt for finished matches using wv_aggr table
    5.2 calculate inventory for finished matches using playout logs and wv table
"""
import holidays
import sys

import pyspark.sql.functions as F
from pyspark.sql.types import *

from config import *
from path import *
from util import *
import new_match

holiday_list = []
cid_mapping = {}
valid_teams = {UNKNOWN_TOKEN: 1}


@F.udf(returnType=IntegerType())
def check_if_focal_tournament(sport_season_name):
    if isinstance(sport_season_name, str):
        sport_season_name = sport_season_name.lower()
        for t in FOCAL_TOURNAMENTS_FOR_FORECASTING:
            if t in sport_season_name:  # sport_season_name is a super-string of tournament
                return 1
        if "india" in sport_season_name and " tour " in sport_season_name:
            return 1
    return 0


# get continent for team
@F.udf(returnType=StringType())
def get_continent(team, tournament_type):
    if tournament_type == "national":
        return "AS"
    elif team in CONTINENT_DIC:
        return CONTINENT_DIC[team]
    else:
        return DEFAULT_CONTINENT


def get_holidays(country, year):
    return [str(x) for x in list(holidays.country_holidays(country, years=year).keys())]


@F.udf(returnType=StringType())
def get_cms_content_id(date, team1, team2, raw_content_id):
    global cid_mapping
    if date in cid_mapping:
        for match in cid_mapping[date]:
            if f"{team1} vs {team2}" in match[1] or f"{team2} vs {team1}" in match[1]:
                return match[0]
            if team1 in SHORT_TEAM_MAPPING and team2 in SHORT_TEAM_MAPPING:
                if f"{SHORT_TEAM_MAPPING[team1]} vs {SHORT_TEAM_MAPPING[team2]}" in match[1] or f"{SHORT_TEAM_MAPPING[team2]} vs {SHORT_TEAM_MAPPING[team1]}" in match[1]:
                    return match[0]
    return raw_content_id


@F.udf(returnType=StringType())
def check_valid_team(team):
    global valid_teams
    if team in valid_teams:
        return team
    else:
        dis = 100000
        for valid_team in valid_teams:
            print(valid_team)
            print(team)
            new_dis = levenshtein_distance(valid_team, team)
            if new_dis < dis:
                dis = new_dis
        if dis <= 1:
            return team + "_invalid_team"
        else:
            return team


# calculate edit distance between two strings
def levenshtein_distance(str1, str2):
    if len(str1) < len(str2):
        return levenshtein_distance(str2, str1)
    if len(str2) == 0:
        return len(str1)
    previous_row = range(len(str2) + 1)
    for i, c1 in enumerate(str1):
        current_row = [i + 1]
        for j, c2 in enumerate(str2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row
    return previous_row[-1]


@F.udf(returnType=StringType())
def correct_team(team):
    global valid_teams
    if team in valid_teams:
        return team
    else:
        dis = 100000
        res = ""
        for valid_team in valid_teams:
            print(valid_team)
            print(team)
            new_dis = levenshtein_distance(valid_team, team)
            if new_dis < dis:
                dis = new_dis
                res = valid_team
        if dis <= 1:
            return res
        else:
            return team


# special case processing when match id is a negative number, mainly happened in tour matches
@F.udf(returnType=StringType())
def extract_match_id(match_id):
    match_id_int = int(match_id)
    if match_id_int < 0:
        return match_id[1:-1]
    else:
        return match_id


# feature processing of request data
def feature_processing(df, run_date):
    run_year = int(run_date[:4])
    global holiday_list, cid_mapping
    holiday_list = get_holidays("IN", run_year) + get_holidays("IN", run_year+1)
    df.groupby('seasonName').count().show(20, False)
    cms_df = load_data_frame(spark, MATCH_CMS_PATH_TEMPL % run_date)\
        .selectExpr('content_id', 'startdate', 'lower(title)').collect()
    for row in cms_df:
        if row[1] not in cid_mapping:
            cid_mapping[row[1]] = []
        cid_mapping[row[1]].append([row[0], row[2]])
    # cid_mapping = {date: list of [content_id, title]}
    feature_df = df \
        .withColumn('date', F.col('matchDate')) \
        .withColumn('tournament', F.expr('lower(seasonName)')) \
        .withColumn('matchId', F.expr('cast(matchId as string)')) \
        .withColumn('matchId', extract_match_id('matchId')) \
        .withColumn('requestId', F.expr('cast(requestId as string)')) \
        .withColumn('content_id', F.concat_ws("#-#", F.col('matchDate'), F.col('matchId'))) \
        .withColumn('vod_type', F.expr('lower(tournamentType)')) \
        .withColumn('match_stage', F.expr('lower(matchType)')) \
        .withColumn('tournament_name', F.expr('lower(tournamentName)')) \
        .withColumn('match_type', F.expr('lower(matchCategory)')) \
        .withColumn('team1', F.expr('lower(team1)')) \
        .withColumn('team1', F.expr(f'if(team1="{UNKNOWN_TOKEN2}", "{UNKNOWN_TOKEN}", team1)')) \
        .withColumn('team2', F.expr('lower(team2)')) \
        .withColumn('team2', F.expr(f'if(team2="{UNKNOWN_TOKEN2}", "{UNKNOWN_TOKEN}", team2)')) \
        .withColumn('team1_check', check_valid_team('team1'))\
        .withColumn('team2_check', check_valid_team('team2'))\
        .withColumn('team1', correct_team('team1'))\
        .withColumn('team2', correct_team('team2'))\
        .withColumn('team1', F.expr('if(date="2023-11-15" or date="2023-11-19", "india", team1)'))\
        .withColumn('team1', F.expr('if(date="2023-11-16", "south africa", team1)'))\
        .withColumn('team2', F.expr('if(date="2023-11-19", "australia", team2)'))\
        .withColumn('team2', F.expr('if(date="2023-11-16", "australia", team2)'))\
        .withColumn('team2', F.expr('if(date="2023-11-15", "new zealand", team2)')) \
        .withColumn('content_id', get_cms_content_id('date', 'team1', 'team2', 'content_id')) \
        .withColumn('if_contain_india_team', F.expr(f'case when team1="india" or team2="india" then "1" '
                                                    f'when team1="{UNKNOWN_TOKEN}" or team2="{UNKNOWN_TOKEN}" then "{UNKNOWN_TOKEN}" '
                                                    f'else "0" end')) \
        .withColumn('if_holiday', check_holiday_udf('matchDate')) \
        .withColumn('if_holiday', F.expr('case when publicHoliday=true then 1 '
                                         'when publicHoliday=false then 0 '
                                         'else if_holiday end')) \
        .withColumn('match_time', F.expr('cast(matchStartHour as int)')) \
        .withColumn('match_time', F.expr('cast(match_time/6 as int)')) \
        .withColumn('if_weekend', F.dayofweek(F.col('matchDate'))) \
        .withColumn('if_weekend', F.expr('if(if_weekend=1 or if_weekend = 7, 1, 0)')) \
        .withColumn('tournament_type', F.expr('case when locate("ipl", tournament) > 0 or locate("ranji trophy", tournament) > 0 then "national" '
                                              'when locate("tour", tournament) > 0 then "tour" '
                                              'else "international" end')) \
        .withColumn('teams', F.array(F.col('team1'), F.col('team2'))) \
        .withColumn('continent1', get_continent('team1', 'tournament_type')) \
        .withColumn('continent2', get_continent('team2', 'tournament_type')) \
        .withColumn('tierOfTeam1', F.expr('lower(tierOfTeam1)')) \
        .withColumn('tierOfTeam1', F.expr(f'if(locate("{UNKNOWN_TOKEN2}", tierOfTeam1) > 0, "{DEFAULT_TIER}", tierOfTeam1)')) \
        .withColumn('tierOfTeam2', F.expr('lower(tierOfTeam2)')) \
        .withColumn('tierOfTeam2', F.expr(f'if(locate("{UNKNOWN_TOKEN2}", tierOfTeam2) > 0, "{DEFAULT_TIER}", tierOfTeam2)')) \
        .withColumn('tierOfTeam1', F.expr(f'if(date="2023-09-14" and team1="pakistan", "{DEFAULT_TIER}", tierOfTeam1)')) \
        .withColumn('tierOfTeam2', F.expr(f'if(date="2023-09-14" and team2="pakistan", "{DEFAULT_TIER}", tierOfTeam2)')) \
        .withColumn('continents', F.array(F.col('continent1'), F.col('continent2'))) \
        .withColumn('teams_tier', F.array(F.col('tierOfTeam1'), F.col('tierOfTeam2'))) \
        .withColumn('free_timer', F.col('svodFreeTimeDuration')) \
        .withColumn('match_duration', F.col('estimatedMatchDuration')) \
        .withColumn('break_duration', F.expr('fixedBreak * averageBreakDuration + adhocBreak * adhocBreakDuration'))\
        .withColumn('if_focal_tournament', check_if_focal_tournament('tournament'))
    for col in FEATURE_COLS:
        if col not in ARRAY_FEATURE_COLS:
            feature_df = feature_df \
                .withColumn(col, F.expr(f'cast({col} as string)')) \
                .withColumn(col, F.array(F.col(col)))
    for col in LABEL_COLS:
        feature_df = feature_df \
            .withColumn(col, F.lit(-1))
    print("feature df")
    feature_df.show(2000, False)
    print("invalid feature df")
    feature_df\
        .where(F.expr('team1_check like "%invalid_team%" or team2_check like "%invalid_team%"'))\
        .select('date', 'content_id', 'team1', 'team2', 'team1_check', 'team2_check')\
        .show(2000, False)
    save_data_frame(feature_df, ALL_MATCH_TABLE_PATH + f"/cd={run_date}")
    return feature_df


# calculate and save avg dau at tournament level
def save_avg_dau_for_each_tournament(dates_for_each_tournament_df, run_date):
    dau_df = load_data_frame(spark, f'{DVV_COMBINE_PATH}cd={run_date}/') \
        .selectExpr('ds as date', 'free_vv', 'sub_vv') \
        .cache()
    res_df = dates_for_each_tournament_df\
        .join(dau_df, 'date') \
        .groupBy('tournament') \
        .agg(F.avg('free_vv').alias('total_frees_number'),
             F.avg('sub_vv').alias('total_subscribers_number'))
    save_data_frame(res_df, f"{AVG_DVV_PATH}/cd={run_date}")
    return res_df


def calculate_avg_dau(previous_train_df, request_df, run_date):
    dates_for_each_tournament_df = previous_train_df.select('date', 'tournament') \
        .union(request_df.select('date', 'tournament')) \
        .distinct()
    avg_dau_df = save_avg_dau_for_each_tournament(dates_for_each_tournament_df, run_date)
    # avg_dau_df.orderBy('tournament').show(200, False)
    return avg_dau_df


def update_avg_dau_label(df, avg_dau_df):
    return df \
        .drop('total_frees_number', 'total_subscribers_number') \
        .join(avg_dau_df, 'tournament')


def update_prediction_dataset(request_df, avg_dau_df):
    prediction_df = request_df.where('matchShouldUpdate=true')
    prediction_df = update_avg_dau_label(prediction_df, avg_dau_df)
    print("prediction df")
    print(prediction_df.count())
    prediction_df.show(200, False)
    if prediction_df.count() > 0:
        save_data_frame(prediction_df, PREDICTION_MATCH_TABLE_PATH + f"/cd={run_date}")


def update_train_dataset(request_df, avg_dau_df, previous_train_df):
    new_match_df = request_df \
        .where('matchHaveFinished=true and if_focal_tournament=1') \
        .join(previous_train_df.select('content_id'), 'content_id', 'left_anti') \
        .select(*MATCH_TABLE_COLS) \
        .where('date not in ("2023-08-30", "2023-08-31", "2023-09-02", "2023-09-10", "2023-09-17")')\
        .cache()
    print("new_match df")
    new_match_df.show(20, False)
    if new_match_df.count() == 0:
        new_train_df = previous_train_df
    else:
        spark = hive_spark("dataset_update")
        the_day_before_run_date = get_date_list(run_date, -2)[0]
        try:
            new_match_df = new_match.add_labels_to_new_matches(spark, the_day_before_run_date, new_match_df)
        except Exception as e:
            print(e)
            new_train_df = previous_train_df
        else:
            new_match_df = update_avg_dau_label(new_match_df, avg_dau_df)
            print("new_match df")
            new_match_df.show(20, False)
            new_train_df = previous_train_df \
                .select(*MATCH_TABLE_COLS)\
                .union(new_match_df.select(*MATCH_TABLE_COLS))\
                .withColumn('frees_watching_match_rate', F.expr('match_active_free_num/total_frees_number')) \
                .withColumn('subscribers_watching_match_rate', F.expr('match_active_sub_num/total_subscribers_number')) \
                .cache()
    new_train_df = new_train_df.where('date not in ("2023-08-30", "2023-08-31", "2023-09-02", "2023-09-17")')
    save_data_frame(new_train_df, TRAIN_MATCH_TABLE_PATH + f"/cd={run_date}")
    new_train_df.groupby('tournament').count().show(200, False)


def get_valid_team_names(previous_train_df):
    train = previous_train_df \
        .withColumn('team1', F.element_at(F.col('teams'), 1)) \
        .withColumn('team2', F.element_at(F.col('teams'), 2))
    global valid_teams
    for t in train.select('team1').collect():
        valid_teams[t[0]] = 1
    for t in train.select('team2').collect():
        valid_teams[t[0]] = 1


# update train dataset and prediction dataset from request data
def update_dataset(run_date):
    # load request data and previous train dataset
    request_df = load_data_frame(spark, f"{PREPROCESSED_INPUT_PATH}cd={run_date}").cache()
    last_update_date = get_last_cd(TRAIN_MATCH_TABLE_PATH, end=run_date, invalid_cd=run_date)
    print(last_update_date)
    # we can union training dataset of 2023-09-17 and 2023-09-18 to get the full training dataset
    previous_train_df = load_data_frame(spark, TRAIN_MATCH_TABLE_PATH + f"/cd={last_update_date}")
    # load_data_frame(spark, TRAIN_MATCH_TABLE_PATH + f"/cd=2023-12-05").where('tournament="england_tour_of_india2021"').show(100, False)
    get_valid_team_names(previous_train_df)
    # feature processing
    request_df = feature_processing(request_df, run_date)
    # calculate avg dau of each tournament
    avg_dau_df = calculate_avg_dau(previous_train_df, request_df, run_date)
    # save future match data for inventory prediction
    update_prediction_dataset(request_df, avg_dau_df)
    # update training dataset according to recently finished match data
    update_train_dataset(request_df, avg_dau_df, previous_train_df)


# check if any feature appear in prediction dataset but not in training dataset
def check_feature_shot_through(run_date):
    train_df = load_data_frame(spark, f"{TRAIN_MATCH_TABLE_PATH}/cd={run_date}").cache()
    prediction_df = load_data_frame(spark, f"{PREDICTION_MATCH_TABLE_PATH}/cd={run_date}").cache()
    for fea in DNN_USED_FEATURES:
        print(fea)
        df = prediction_df\
            .select(F.explode(F.col(fea)).alias(fea))\
            .distinct()\
            .join(train_df.select(F.explode(F.col(fea)).alias(fea)).distinct(), fea, 'left_anti')
        if df.count() > 0:
            publish_to_slack(topic=SLACK_NOTIFICATION_TOPIC, title=f"ALERT: feature {fea} on {run_date} not shot in training dataset",
                             output_df=df, region=REGION)


check_holiday_udf = F.udf(lambda date: 1 if date in holiday_list else 0, IntegerType())


if __name__ == '__main__':
    run_date = sys.argv[1]
    # run_date = "2023-06-30"
    update_dataset(run_date)
    if check_s3_path_exist(f"{PREDICTION_MATCH_TABLE_PATH}/cd={run_date}/"):
        check_feature_shot_through(run_date)
    slack_notification(topic=SLACK_NOTIFICATION_TOPIC, region=REGION,
                       message=f"feature processing on {run_date} is done.")


