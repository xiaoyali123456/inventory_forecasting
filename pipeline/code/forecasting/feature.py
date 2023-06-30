from path import *
from util import *
from config import *


# remove invalid char in title
def simple_title(title):
    title = title.strip().lower()
    if title.find(": ") > -1:
        title = title.split(": ")[-1]
    if title.find(", ") > -1:
        title = title.split(", ")[0]
    teams = sorted(title.split(" vs "))
    for i in range(len(teams)):
        if teams[i] in invalid_team_mapping:
            teams[i] = invalid_team_mapping[teams[i]]
    return teams[0] + " vs " + teams[1]


# get continent for team
def get_continent(team, tournament_type):
    if tournament_type == "national":
        return "AS"
    elif team in continent_dic:
        return {continent_dic[team]}
    else:
        return unknown_token


# get continents for teams
def get_continents(teams, tournament_type):
    teams = teams.split(" vs ")
    if tournament_type == "national":
        return "AS vs AS"
    elif teams[0] in continent_dic and teams[1] in continent_dic:
        return f"{continent_dic[teams[0]]} vs {continent_dic[teams[1]]}"
    else:
        return ""


# get tiers for teams
def get_teams_tier(teams):
    teams = teams.split(" vs ")
    if teams[0] in tiers_dic and teams[1] in tiers_dic:
        return f"{tiers_dic[teams[0]]} vs {tiers_dic[teams[1]]}"
    else:
        return ""


# hots_num indicates the dimension of the vector
def generate_hot_vector(hots, hots_num):
    res = [0 for i in range(hots_num)]
    for hot in hots:
        if hot >= 0:
            res[hot] += 1
    return res


# convert string-based features to vector-based features
def add_categorical_features(feature_df):
    # Add rank column
    df = feature_df\
        .withColumn('rank', F.expr('row_number() over (partition by tournament order by date)'))\
        .cache()
    # Process multi-hot columns
    for col in multi_hot_cols:
        print(col)
        feature_dic_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + "/feature_mapping/" + col).cache()
        exploded_df = df\
            .select('tournament', 'rank', F.split(col, " vs ").alias(f"{col}_list")) \
            .select('tournament', 'rank', F.explode(f"{col}_list").alias(f"{col}_item")) \
            .join(feature_dic_df, f"{col}_item", 'left') \
            .fillna(-1, [f'{col}_hots']) \
            .groupBy('tournament', 'rank') \
            .agg(F.collect_list(f"{col}_hots").alias(f"{col}_hots"))
        df = df.join(exploded_df, ['tournament', 'rank'])
    print("multi hots feature processed done!")
    # Process one-hot columns
    for col in one_hot_cols:
        print(col)
        feature_dic_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + "/feature_mapping/" + col).cache()
        df = df\
            .join(feature_dic_df, col, 'left') \
            .fillna(-1, [f'{col}_hots']) \
            .withColumn(f"{col}_hots", F.array(F.col(f"{col}_hots")))
    print("one hot feature processed done!")
    # Process numerical columns
    for col in numerical_cols:
        print(col)
        df = df\
            .withColumn(f"{col}_hots", F.array(F.col(f"{col}")))
    print("all hots feature processed done!")
    return df


def save_base_dataset(path_suffix):
    df = load_data_frame(spark, live_ads_inventory_forecasting_complete_feature_path + "/all_features_hots_format_with_avg_au_sub_free_num" + path_suffix)\
        .where('date >= "2019-05-30"')\
        .cache()
    base_path_suffix = "/all_features_hots_format"
    contents = df.select('date', 'content_id').distinct().collect()
    for content in contents:
        print(content)
        date = content[0]
        content_id = content[1]
        res_df = df\
            .where(f'date="{date}" and content_id="{content_id}"') \
            .withColumnRenamed('hostar_influence', 'hotstar_influence')\
            .withColumnRenamed('hostar_influence_hots', 'hotstar_influence_hots')\
            .withColumnRenamed('hostar_influence_hots_num', 'hotstar_influence_hots_num')\
            .withColumnRenamed('hostar_influence_hot_vector', 'hotstar_influence_hot_vector')\
            .withColumn("hotstar_influence_hots_num", F.lit(1)) \
            .withColumn('frees_watching_match_rate', F.bround(F.col('frees_watching_match_rate'), 2)) \
            .withColumn('subscribers_watching_match_rate', F.bround(F.col('subscribers_watching_match_rate'), 2)) \
            .withColumn('watch_time_per_free_per_match', F.bround(F.col('watch_time_per_free_per_match'), 2)) \
            .withColumn('watch_time_per_subscriber_per_match', F.bround(F.col('watch_time_per_subscriber_per_match'), 2))
        if path_suffix.find('free_timer') > -1:
            res_df = res_df\
                .withColumn("free_timer_hot_vector", F.array(F.col('free_timer'))) \
                .withColumn("free_timer_hots_num", F.lit(1))
        save_data_frame(res_df, pipeline_base_path + base_path_suffix + path_suffix + f"/cd={date}/contentid={content_id}")


def generate_prediction_dataset(DATE):
    request_df = load_data_frame(spark, f"{inventory_forecast_request_path}/cd={DATE}").cache()
    feature_df = request_df\
        .withColumn('date', F.col('matchDate'))\
        .withColumn('tournament', F.expr('lower(seasonName)'))\
        .withColumn('matchId', F.expr('cast(matchId as string)')) \
        .withColumn('content_id', F.concat_ws("#-#", F.col('requestId'), F.col('matchId')))\
        .withColumn('vod_type', F.expr('lower(tournamentType)'))\
        .withColumn('match_stage', F.expr('lower(matchType)'))\
        .withColumn('tournament_name', F.expr('lower(tournamentName)'))\
        .withColumn('match_type', F.expr('lower(matchCategory)'))\
        .withColumn('team1', F.expr('lower(team1)'))\
        .withColumn('team2', F.expr('lower(team2)'))\
        .withColumn('if_contain_india_team', F.expr(f'if(team1="india" or team2="india", "1", '
                                                    f'if(team1="{unknown_token}" or team2="{unknown_token}", "{unknown_token}", "0"))'))\
        .withColumn('if_holiday', check_holiday_udf('matchDate'))\
        .withColumn('match_time', F.expr('cast(matchStartHour/6 as int)')) \
        .withColumn('if_weekend', F.dayofweek(F.col('matchDate'))) \
        .withColumn('if_weekend', F.expr('if(if_weekend=1 or if_weekend = 7, 1, 0)')) \
        .withColumn('tournament_type', F.expr('if(locate("ipl", tournament) > 0, "national", if(locate("tour", tournament) > 0, "tour", "international"))'))\
        .withColumn('teams', F.array(F.col('team1'), F.col('team2'))) \
        .withColumn('continent1', get_continent_udf('team1', 'tournament_type')) \
        .withColumn('continent2', get_continent_udf('team2', 'tournament_type')) \
        .withColumn('tierOfTeam1', F.expr('lower(tierOfTeam1)')) \
        .withColumn('tierOfTeam2', F.expr('lower(tierOfTeam2)')) \
        .withColumn('continents', F.array(F.col('continent1'), F.col('continent2')))\
        .withColumn('teams_tier', F.array(F.col('tierOfTeam1'), F.col('tierOfTeam2')))\
        .withColumn('free_timer', F.col('svodFreeTimeDuration'))\
        .withColumn('match_duration', F.col('estimatedMatchDuration'))\
        .withColumn('break_duration', F.expr('fixedBreak * averageBreakDuration + adhocBreak * adhocBreakDuration'))
    for col in feature_cols:
        if col not in array_feature_cols:
            feature_df = feature_df\
                .withColumn(col, F.expr(f'cast({col} as string)'))\
                .withColumn(col, F.array(F.col(col)))
    for col in label_cols:
        feature_df = feature_df\
            .withColumn(col, F.lit(-1))
    save_data_frame(feature_df, prediction_feature_path + f"/cd={DATE}")
    prediction_df = feature_df\
        .where('matchShouldUpdate=true')\
        .select(*match_table_cols)
    prediction_df.show(20, False)
    save_data_frame(prediction_df, prediction_match_table_path + f"/cd={DATE}")
    new_match_df = feature_df.where('matchHaveFinished=true').select(*match_table_cols).cache()
    if new_match_df.count() == 0:
        YESTERDAY = get_date_list(DATE, -2)[0]
        save_data_frame(load_data_frame(spark, train_match_table_path + f"/cd={YESTERDAY}"), train_match_table_path + f"/cd={DATE}")
    else:
        pass


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


simple_title_udf = F.udf(simple_title, StringType())
get_teams_tier_udf = F.udf(get_teams_tier, StringType())
get_continent_udf = F.udf(get_continent, StringType())
get_continents_udf = F.udf(get_continents, StringType())
generate_hot_vector_udf = F.udf(generate_hot_vector, ArrayType(IntegerType()))
check_holiday_udf = F.udf(check_holiday, IntegerType())

# save_base_dataset("_and_simple_one_hot")
# save_base_dataset("_and_free_timer_and_simple_one_hot")
# save_base_dataset("_full_avod_and_simple_one_hot")
# main(spark, date, content_id, tournament_name, match_type, venue, match_stage, gender_type, vod_type, match_start_time_ist)

# print("argv", sys.argv)

if __name__ == '__main__':
    DATE = sys.argv[1]
    # DATE = "2023-06-30"
    generate_prediction_dataset(DATE)
    slack_notification(topic=slack_notification_topic, region=region,
                       message=f"feature processing on {DATE} is done.")


