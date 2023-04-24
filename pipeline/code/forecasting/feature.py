from path import *
from util import *
from config import *


def generate_hot_vector(hots, hots_num):
    res = [0 for i in range(hots_num)]
    for hot in hots:
        if hot >= 0:
            res[hot] += 1
    return res


generate_hot_vector_udf = F.udf(generate_hot_vector, ArrayType(IntegerType()))


def simple_title(title):
    title = title.strip().lower()
    if title.find(": ") > -1:
        title = title.split(": ")[-1]
    if title.find(", ") > -1:
        title = title.split(", ")[0]
    teams = sorted(title.split(" vs "))
    for i in range(len(teams)):
        if teams[i] in unvalid_team_mapping:
            teams[i] = unvalid_team_mapping[teams[i]]
    return teams[0] + " vs " + teams[1]


def get_continents(teams, tournament_type):
    teams = teams.split(" vs ")
    if tournament_type == "national":
        return "AS vs AS"
    elif teams[0] in continent_dic and teams[1] in continent_dic:
        return f"{continent_dic[teams[0]]} vs {continent_dic[teams[1]]}"
    else:
        return ""


def get_teams_tier(teams):
    teams = teams.split(" vs ")
    if teams[0] in tiers_dic and teams[1] in tiers_dic:
        return f"{tiers_dic[teams[0]]} vs {tiers_dic[teams[1]]}"
    else:
        return ""


def generate_hot_vector(hots, hots_num):
    res = [0 for i in range(hots_num)]
    for hot in hots:
        if hot >= 0:
            res[hot] += 1
    return res


def add_hots_features(feature_df, type="train", root_path=""):
    col_num_dic = {}
    df = feature_df\
        .withColumn('rank', F.expr('row_number() over (partition by tournament order by date)'))\
        .cache()
    df.groupBy('tournament').count().orderBy('tournament').show()
    # save_data_frame(df, root_path + "/base_features")
    print(df.count())
    for col in multi_hot_cols:
        print(col)
        df2 = df\
            .select('tournament', 'rank', f"{col}")\
            .withColumn(f"{col}_list", F.split(F.col(col), " vs "))\
            .withColumn(f"{col}_item", F.explode(F.col(f"{col}_list")))\
            .cache()
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
        # unvalid_num = df2.select('tournament', 'rank', f"{col}_item").join(col_df, f"{col}_item", 'left').where(f"{col}_hots is null").count()
        # if unvalid_num > 0:
        #     print(unvalid_num)
        df = df\
            .join(df2
                  .select('tournament', 'rank', f"{col}_item")
                  .join(col_df, f"{col}_item", 'left')
                  .fillna(-1, [f'{col}_hots'])
                  .groupBy('tournament', 'rank')
                  .agg(F.collect_list(f"{col}_hots").alias(f"{col}_hots")), ['tournament', 'rank']) \
            .withColumn(f"{col}_hots_num", F.lit(col_num))
        # print(df.count())
    save_data_frame(df, root_path + "/base_features_with_all_features_multi_hots_simple")
    print("multi hots feature simple processed done!")
    df = load_data_frame(spark, root_path + "/base_features_with_all_features_multi_hots_simple").cache()
    # df.show()
    for col in multi_hot_cols:
        print(col)
        # df.select(f"{col}_hots", f"{col}_hots_num").show(20, False)
        df = df\
            .withColumn(f"{col}_hot_vector", generate_hot_vector_udf(f"{col}_hots", f"{col}_hots_num"))
        # df.show()
    # print(df.count())
    save_data_frame(df, root_path + "/base_features_with_all_features_multi_hots")
    print("multi hots feature processed done!")
    df = load_data_frame(spark, root_path + "/base_features_with_all_features_multi_hots").cache()
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
        # unvalid_num = df.join(col_df, col, 'left').where(f"{col}_hots is null").count()
        # if unvalid_num > 0:
        #     print(unvalid_num)
        df = df\
            .join(col_df, col, 'left') \
            .fillna(-1, [f'{col}_hots']) \
            .withColumn(f"{col}_hots_num", F.lit(col_num))
    for col in one_hot_cols:
        print(col)
        df = df\
            .withColumn(f"{col}_hot_vector", generate_hot_vector_udf(f"{col}_hots", f"{col}_hots_num"))
    for col in ['hostar_influence']:
        print(col)
        df = df.withColumn(f"{col}_hot_vector", F.array(F.col(f"{col}")))
    df.groupBy('tournament').count().orderBy('tournament').show()
    save_data_frame(df, root_path + "/base_features_with_all_features_hots")
    print("all hots feature processed done!")
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


strip_udf = F.udf(lambda x: x.strip(), StringType())
simple_title_udf = F.udf(simple_title, StringType())
get_teams_tier_udf = F.udf(get_teams_tier, StringType())
get_continents_udf = F.udf(get_continents, StringType())


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
