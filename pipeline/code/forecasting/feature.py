from config import *
from new_match import *
from common import get_last_cd


# get continent for team
def get_continent(team, tournament_type):
    if tournament_type == "national":
        return "AS"
    elif team in CONTINENT_DIC:
        return CONTINENT_DIC[team]
    else:
        return UNKNOWN_TOKEN


# feature processing of request data
def feature_processing(df):
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


# calculate and save avg vv at tournament level
def save_avg_vv_for_each_tournament(dates_for_each_tournament_df, run_date):
    vv_df = load_data_frame(spark, f'{DVV_COMBINE_PATH}cd={run_date}/') \
        .withColumn('free_vv', F.expr('vv - sub_vv')) \
        .selectExpr('ds as date', 'free_vv', 'sub_vv') \
        .cache()
    res_df = dates_for_each_tournament_df\
        .join(vv_df, 'date') \
        .groupBy('tournament') \
        .agg(F.avg('free_vv').alias('total_frees_number'),
             F.avg('sub_vv').alias('total_subscribers_number'))
    save_data_frame(res_df, f"{AVG_DVV_PATH}/cd={run_date}")


# update train dataset and prediction dataset from request data
def update_dataset(run_date):
    request_df = load_data_frame(spark, f"{INVENTORY_FORECAST_REQUEST_PATH}/cd={run_date}").cache()
    # feature processing
    feature_df = feature_processing(request_df)

    # save avg dau of each tournament
    last_update_date = get_last_cd(TRAIN_MATCH_TABLE_PATH, invalid_cd=run_date)
    previous_train_df = load_data_frame(spark, TRAIN_MATCH_TABLE_PATH + f"/cd={last_update_date}")
    dates_for_each_tournament_df = previous_train_df.select('date', 'tournament') \
        .union(feature_df.select('date', 'tournament')) \
        .distinct()
    save_avg_vv_for_each_tournament(dates_for_each_tournament_df, run_date)

    # update total_frees_number and total_subscribers_number by new dau predictions
    avg_dau_df = load_data_frame(spark, f"{AVG_DVV_PATH}/cd={run_date}")
    feature_df = feature_df\
        .drop('total_frees_number', 'total_subscribers_number')\
        .join(avg_dau_df, 'tournament')
    save_data_frame(feature_df, PREDICTION_FEATURE_PATH + f"/cd={run_date}")

    # save future match data for inventory prediction
    prediction_df = feature_df\
        .where('matchShouldUpdate=true')\
        .select(*MATCH_TABLE_COLS)
    prediction_df.show(20, False)
    save_data_frame(prediction_df, PREDICTION_MATCH_TABLE_PATH + f"/cd={run_date}")

    # update training dataset according to recently finished match data
    new_match_df = feature_df\
        .where('matchHaveFinished=true')\
        .select(*MATCH_TABLE_COLS)\
        .cache()
    if new_match_df.count() == 0:
        new_train_df = previous_train_df
    else:
        the_day_before_run_date = get_date_list(run_date, -2)[0]
        new_match_df = add_labels_to_new_matches(spark, the_day_before_run_date, new_match_df)
        new_train_df = previous_train_df.select(*MATCH_TABLE_COLS).union(new_match_df.select(*MATCH_TABLE_COLS))
    new_train_df = new_train_df\
        .drop('total_frees_number', 'total_subscribers_number')\
        .join(avg_dau_df, 'tournament') \
        .withColumn('frees_watching_match_rate', F.expr('match_active_free_num/total_frees_number')) \
        .withColumn('subscribers_watching_match_rate', F.expr('match_active_sub_num/total_subscribers_number'))\
        .cache()
    save_data_frame(new_train_df, TRAIN_MATCH_TABLE_PATH + f"/cd={run_date}")


def check_holiday(date):
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
    return 1 if date in india_holidays else 0


get_continent_udf = F.udf(get_continent, StringType())
check_holiday_udf = F.udf(check_holiday, IntegerType())

# save_base_dataset("_full_avod_and_simple_one_hot")


if __name__ == '__main__':
    run_date = sys.argv[1]
    # run_date = "2023-06-30"
    update_dataset(run_date)
    slack_notification(topic=SLACK_NOTIFICATION_TOPIC, region=REGION,
                       message=f"feature processing on {run_date} is done.")


