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


def update_feature_dic(df, col):
    previous_col_df = load_data_frame(spark, feature_dic_path + f"/cd={YESTERDAY}/col={col}")
    new_col_df = previous_col_df\
        .select(col)\
        .union(df.select(col))\
        .distinct()\
        .withColumn('tag', F.lit(1))\
        .withColumn(f"{col}_hots", F.expr(f'row_number() over (partition by tag order by {col})'))\
        .withColumn(f"{col}_hots", F.expr(f'{col}_hots - 1'))\
        .drop('tag')\
        .cache()
    save_data_frame(new_col_df, feature_dic_path + f"/cd={DATE}/col={col}")


# convert string-based features to vector-based features
def update_categorical_features(feature_df):
    col_num_dic = {}
    df = feature_df\
        .withColumn('match_id', F.expr('row_number() over (partition by tournament order by date)'))\
        .cache()
    print(df.count())
    # multi_hot_cols = ['teams', 'continents', 'teams_tier'], which means the hot number > 1
    for col in multi_hot_cols:
        print(col)
        df2 = df\
            .select('tournament', 'match_id', f"{col}")\
            .withColumn(f"{col}_list", F.split(F.col(col), " vs "))\
            .withColumn(f"{col}_item", F.explode(F.col(f"{col}_list")))\
            .cache()
        # save the feature mappings from string to index
        update_feature_dic(df2, f"{col}_item")
        col_df = load_data_frame(spark, feature_dic_path + f"/cd={DATE}/col={col}").cache()
        col_num = col_df.count()
        col_num_dic[col] = col_num
        df = df\
            .join(df2
                  .select('tournament', 'match_id', f"{col}_item")
                  .join(col_df, f"{col}_item")
                  .groupBy('tournament', 'match_id')
                  .agg(F.collect_list(f"{col}_hots").alias(f"{col}_hots")), ['tournament', 'match_id']) \
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
        # save the feature mappings from string to index
        df2 = df.select(col).cache()
        update_feature_dic(df2, col)
        col_df = load_data_frame(spark, feature_dic_path + f"/cd={DATE}/col={col}").cache()
        col_num = col_df.count()
        col_df = col_df.withColumn(f"{col}_hots", F.array(F.col(f"{col}_hots")))
        col_num_dic[col] = col_num
        df = df\
            .join(col_df, col) \
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


def add_new_matches_to_train_dataset(spark, date, content_id, tournament_name, match_type,
         venue, match_stage, gender_type, vod_type, match_start_time_ist):
    spark.stop()
    spark = hive_spark('statistics')
    match_df = load_hive_table(spark, "in_cms.match_update_s3")\
        .where(f'content_id = "{content_id}"')\
        .selectExpr('contentid as content_id', 'title', 'shortsummary')\
        .distinct()\
        .cache()
    active_user_df = load_data_frame(spark, f'{dau_truth_path}/cd={DATE}')\
        .withColumn('total_frees_number', F.expr('vv - sub_vv'))\
        .withColumn('content_id', F.lit(content_id))\
        .selectExpr('ds as date', 'content_id', 'total_frees_number', 'sub_vv as total_subscribers_number')\
        .where(f'date = "{date}"')\
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
        .withColumn('hotstar_influence', F.expr('year - 2016')) \
        .withColumn('match_stage', F.lit(match_stage.lower())) \
        .withColumn('vod_type', F.lit(vod_type.lower())) \
        .withColumn('gender_type', F.lit(gender_type.lower())) \
        .withColumn('teams', simple_title_udf('title')) \
        .withColumn('continents', get_continents_udf('teams', 'tournament_type')) \
        .withColumn('teams_tier', get_teams_tier_udf('teams')) \
        .withColumn('free_timer', F.expr('if(vod_type="avod", 1000, 5)')) \
        .cache()
    feature_df = update_categorical_features(feature_df)
    save_data_frame(feature_df, training_data_path + f"/cd={date}/contentid={content_id}")


simple_title_udf = F.udf(simple_title, StringType())
get_teams_tier_udf = F.udf(get_teams_tier, StringType())
get_continents_udf = F.udf(get_continents, StringType())
generate_hot_vector_udf = F.udf(generate_hot_vector, ArrayType(IntegerType()))


if __name__ == '__main__':
    DATE = sys.argv[1]
    YESTERDAY = get_date_list(DATE, -2)[0]
    pass

