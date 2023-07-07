import holidays

from config import *
from path import *
from util import *
from common import get_last_cd
import new_match

holiday_list = []


# get continent for team
def get_continent(team, tournament_type):
    if tournament_type == "national":
        return "AS"
    elif team in CONTINENT_DIC:
        return CONTINENT_DIC[team]
    else:
        return UNKNOWN_TOKEN


def get_holidays(country, year):
    return [str(x) for x in list(holidays.country_holidays(country, years=year).keys())]


# feature processing of request data
def feature_processing(df, run_date):
    run_year = int(run_date[:4])
    global holiday_list
    holiday_list = get_holidays("IN", run_year) + get_holidays("IN", run_year+1)
    feature_df = df \
        .withColumn('date', F.col('matchDate')) \
        .withColumn('tournament', F.expr('lower(seasonName)')) \
        .withColumn('matchId', F.expr('cast(matchId as string)')) \
        .withColumn('requestId', F.expr('cast(requestId as string)')) \
        .withColumn('content_id', F.concat_ws("#-#", F.col('requestId'), F.col('matchId'))) \
        .withColumn('vod_type', F.expr('lower(tournamentType)')) \
        .withColumn('match_stage', F.expr('lower(matchType)')) \
        .withColumn('tournament_name', F.expr('lower(tournamentName)')) \
        .withColumn('match_type', F.expr('lower(matchCategory)')) \
        .withColumn('team1', F.expr('lower(team1)')) \
        .withColumn('team2', F.expr('lower(team2)')) \
        .withColumn('if_contain_india_team', F.expr(f'if(team1="india" or team2="india", "1", '
                                                    f'if(team1="{UNKNOWN_TOKEN}" or team2="{UNKNOWN_TOKEN}", "{UNKNOWN_TOKEN}", "0"))')) \
        .withColumn('if_holiday', check_holiday_udf('matchDate')) \
        .withColumn('match_time', F.expr('cast(matchStartHour/6 as int)')) \
        .withColumn('if_weekend', F.dayofweek(F.col('matchDate'))) \
        .withColumn('if_weekend', F.expr('if(if_weekend=1 or if_weekend = 7, 1, 0)')) \
        .withColumn('tournament_type', F.expr('if(locate("ipl", tournament) > 0, "national", if(locate("tour", tournament) > 0, "tour", "international"))')) \
        .withColumn('teams', F.array(F.col('team1'), F.col('team2'))) \
        .withColumn('continent1', get_continent_udf('team1', 'tournament_type')) \
        .withColumn('continent2', get_continent_udf('team2', 'tournament_type')) \
        .withColumn('tierOfTeam1', F.expr('lower(tierOfTeam1)')) \
        .withColumn('tierOfTeam2', F.expr('lower(tierOfTeam2)')) \
        .withColumn('continents', F.array(F.col('continent1'), F.col('continent2'))) \
        .withColumn('teams_tier', F.array(F.col('tierOfTeam1'), F.col('tierOfTeam2'))) \
        .withColumn('free_timer', F.col('svodFreeTimeDuration')) \
        .withColumn('match_duration', F.col('estimatedMatchDuration')) \
        .withColumn('break_duration', F.expr('fixedBreak * averageBreakDuration + adhocBreak * adhocBreakDuration'))

    for col in FEATURE_COLS:
        if col not in ARRAY_FEATURE_COLS:
            feature_df = feature_df \
                .withColumn(col, F.expr(f'cast({col} as string)')) \
                .withColumn(col, F.array(F.col(col)))

    for col in LABEL_COLS:
        feature_df = feature_df \
            .withColumn(col, F.lit(-1))

    return feature_df


# calculate and save avg dau at tournament level
def save_avg_dau_for_each_tournament(dates_for_each_tournament_df, run_date):
    dau_df = load_data_frame(spark, f'{DVV_COMBINE_PATH}cd={run_date}/') \
        .withColumn('free_vv', F.expr('vv - sub_vv')) \
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
    return avg_dau_df


def update_avg_dau_label(df, avg_dau_df):
    return df \
        .drop('total_frees_number', 'total_subscribers_number') \
        .join(avg_dau_df, 'tournament')


def update_prediction_dataset(request_df, avg_dau_df):
    prediction_df = request_df \
        .where('matchShouldUpdate=true')
    prediction_df = update_avg_dau_label(prediction_df, avg_dau_df)
    prediction_df.show(20, False)
    if prediction_df.count() > 0:
        save_data_frame(prediction_df, PREDICTION_MATCH_TABLE_PATH + f"/cd={run_date}")


def update_train_dataset(request_df, avg_dau_df, previous_train_df):
    new_match_df = request_df \
        .where('matchHaveFinished=true') \
        .join(previous_train_df.select('content_id'), 'content_id', 'left_anti') \
        .select(*MATCH_TABLE_COLS) \
        .cache()
    if new_match_df.count() == 0:
        new_train_df = previous_train_df
    else:
        the_day_before_run_date = get_date_list(run_date, -2)[0]
        new_match_df = new_match.add_labels_to_new_matches(spark, the_day_before_run_date, new_match_df)
        new_train_df = previous_train_df.select(*MATCH_TABLE_COLS).union(new_match_df.select(*MATCH_TABLE_COLS))
    new_train_df = update_avg_dau_label(new_train_df, avg_dau_df)
    new_train_df = new_train_df \
        .withColumn('frees_watching_match_rate', F.expr('match_active_free_num/total_frees_number')) \
        .withColumn('subscribers_watching_match_rate', F.expr('match_active_sub_num/total_subscribers_number')) \
        .cache()
    save_data_frame(new_train_df, TRAIN_MATCH_TABLE_PATH + f"/cd={run_date}")


# update train dataset and prediction dataset from request data
def update_dataset(run_date):
    # load request data and previous train dataset
    request_df = load_data_frame(spark, f"{INVENTORY_FORECAST_REQUEST_PATH}/cd={run_date}").cache()
    last_update_date = get_last_cd(TRAIN_MATCH_TABLE_PATH, invalid_cd=run_date)
    previous_train_df = load_data_frame(spark, TRAIN_MATCH_TABLE_PATH + f"/cd={last_update_date}")

    # feature processing
    request_df = feature_processing(request_df, run_date)

    # calculate avg dau of each tournament
    avg_dau_df = calculate_avg_dau(previous_train_df, request_df, run_date)

    # save future match data for inventory prediction
    update_prediction_dataset(request_df, avg_dau_df)

    # update training dataset according to recently finished match data
    update_train_dataset(request_df, avg_dau_df, previous_train_df)


get_continent_udf = F.udf(get_continent, StringType())
check_holiday_udf = F.udf(lambda date: 1 if date in holiday_list else 0, IntegerType())


if __name__ == '__main__':
    run_date = sys.argv[1]
    # run_date = "2023-06-30"
    update_dataset(run_date)
    slack_notification(topic=SLACK_NOTIFICATION_TOPIC, region=REGION,
                       message=f"feature processing on {run_date} is done.")


