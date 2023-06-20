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
    col_num_dic = {}
    df = feature_df\
        .withColumn('rank', F.expr('row_number() over (partition by tournament order by date)'))\
        .cache()
    df.groupBy('tournament').count().orderBy('tournament').show()
    print(df.count())
    # multi_hot_cols = ['teams', 'continents', 'teams_tier'], which means the hot number > 1
    for col in multi_hot_cols:
        print(col)
        df2 = df\
            .select('tournament', 'rank', f"{col}")\
            .withColumn(f"{col}_list", F.split(F.col(col), " vs "))\
            .withColumn(f"{col}_item", F.explode(F.col(f"{col}_list")))\
            .cache()
        col_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + "/feature_mapping/" + col).cache()
        col_num = col_df.count()
        col_num_dic[col] = col_num
        df = df\
            .join(df2
                  .select('tournament', 'rank', f"{col}_item")
                  .join(col_df, f"{col}_item", 'left')
                  .fillna(-1, [f'{col}_hots'])
                  .groupBy('tournament', 'rank')
                  .agg(F.collect_list(f"{col}_hots").alias(f"{col}_hots")), ['tournament', 'rank']) \
            .withColumn(f"{col}_hots_num", F.lit(col_num))
    save_data_frame(df, pipeline_data_tmp_path + "/features_with_multi_hots_simple")
    print("multi hots feature simple processed done!")
    df = load_data_frame(spark, pipeline_data_tmp_path + "/features_with_multi_hots_simple").cache()
    for col in multi_hot_cols:
        print(col)
        df = df.withColumn(f"{col}_hot_vector", generate_hot_vector_udf(f"{col}_hots", f"{col}_hots_num"))
    save_data_frame(df, pipeline_data_tmp_path + "/features_with_multi_hots")
    print("multi hots feature processed done!")
    df = load_data_frame(spark, pipeline_data_tmp_path + "/features_with_multi_hots").cache()
    # one hot means the hot number = 1
    for col in one_hot_cols:
        print(col)
        col_df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + "/feature_mapping/" + col).cache()
        col_num = col_df.count()
        col_df = col_df.withColumn(f"{col}_hots", F.array(F.col(f"{col}_hots")))
        col_num_dic[col] = col_num
        df = df\
            .join(col_df, col, 'left') \
            .fillna(-1, [f'{col}_hots']) \
            .withColumn(f"{col}_hots_num", F.lit(col_num))
    for col in one_hot_cols:
        print(col)
        df = df.withColumn(f"{col}_hot_vector", generate_hot_vector_udf(f"{col}_hots", f"{col}_hots_num"))
    for col in numerical_cols:
        print(col)
        df = df\
            .withColumn(f"{col}_hot_vector", F.array(F.col(f"{col}"))) \
            .withColumn(f"{col}_hots_num", F.lit(1))
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


def generate_prediction_dataset(today, config):
    res = []
    cols = ['request_id', 'tournament_name', 'tournament', 'match_type', 'vod_type', 'venue_detail', 'free_timer',
            'match_id', 'content_id', 'match_stage', 'date', 'year', 'match_start_time', 'title', 'if_holiday', 'match_duration', 'break_duration']
    for request in config:
        request_id = request['id']
        tournament_name = request['tournamentName'].lower()
        tournament = request['seasonName'].replace(" ", "_").lower()
        vod_type = request['tournamentType'].lower()
        venue_detail = request['tournamentLocation'].lower()
        free_timer = request['svodFreeTimeDuration']
        for match in request['matchDetails']:
            match_id = match['matchId']
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
                        match_id, content_id, match_stage, date, year, match_start_time, title, if_holiday, match_duration, break_duration))
    feature_df = spark.createDataFrame(res, cols) \
        .withColumn('total_frees_number', F.lit(-1)) \
        .withColumn('total_subscribers_number', F.lit(-1)) \
        .withColumn(free_rate_label, F.lit(-1.0)) \
        .withColumn(free_wt_label, F.lit(-1.0)) \
        .withColumn(sub_rate_label, F.lit(-1.0)) \
        .withColumn(sub_wt_label, F.lit(-1.0)) \
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
        .withColumn('hotstar_influence', F.expr('year - 2016')) \
        .withColumn('teams', simple_title_udf('title')) \
        .withColumn('continents', get_continents_udf('teams', 'tournament_type')) \
        .withColumn('teams_tier', get_teams_tier_udf('teams')) \
        .withColumn('free_timer', F.expr('if(vod_type="avod", 1000, free_timer)')) \
        .cache()
    feature_df = add_categorical_features(feature_df)
    save_data_frame(feature_df, prediction_feature_path + f"/cd={today}")
    # for col in one_hot_cols:
    #     if col != "if_contain_india_team" and feature_df.select(f"{col}_hots_num").distinct().collect()[0][0] == 2:
    #         print(col)
    #         feature_df = feature_df \
    #             .withColumn(f"{col}_hots_num", F.lit(1)) \
    #             .withColumn(f"{col}_hot_vector", F.col(f"{col}_hots"))
    # save_data_frame(feature_df, pipeline_base_path + base_path_suffix + "_and_simple_one_hot" + f"/cd={today}")
    # cols = [col + "_hot_vector" for col in one_hot_cols + multi_hot_cols + numerical_cols]
    # df = load_data_frame(spark, pipeline_base_path + base_path_suffix + "_and_simple_one_hot" + f"/cd={today}") \
    #     .drop(*cols)
    # df.orderBy('date', 'content_id').show(3000, False)
    return feature_df


simple_title_udf = F.udf(simple_title, StringType())
get_teams_tier_udf = F.udf(get_teams_tier, StringType())
get_continents_udf = F.udf(get_continents, StringType())
generate_hot_vector_udf = F.udf(generate_hot_vector, ArrayType(IntegerType()))


# save_base_dataset("_and_simple_one_hot")
# save_base_dataset("_and_free_timer_and_simple_one_hot")
# save_base_dataset("_full_avod_and_simple_one_hot")
# main(spark, date, content_id, tournament_name, match_type, venue, match_stage, gender_type, vod_type, match_start_time_ist)

# print("argv", sys.argv)

if __name__ == '__main__':
    DATE = sys.argv[1]
    config = load_requests(DATE)
    generate_prediction_dataset(DATE, config=config)
