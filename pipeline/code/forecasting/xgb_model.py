from path import *
from util import *
from config import *


def load_dataset(feature_df, test_tournament, sorting=False, repeat_num_col="", mask_cols=[], mask_condition="", mask_rate=1, wc2019_test_tag=1):
    if test_tournament == "wc2019":
        if sorting:
            sample_tag = wc2019_test_tag
        else:
            sample_tag = 3 - wc2019_test_tag
            # sample_tag = "1, 2"
    else:
        sample_tag = "1, 2"
    if sorting:
        if test_tournament == "":
            new_feature_df = feature_df \
                .where(f'sample_tag in (0, {sample_tag})') \
                .cache()
        else:
            new_feature_df = feature_df \
                .where(f'tournament = "{test_tournament}" and sample_tag in (0, {sample_tag})') \
                .cache()
    else:
        new_feature_df = feature_df \
            .where(f'tournament != "{test_tournament}" and sample_tag in (0, {sample_tag})') \
            .cache()
        if test_tournament == "wc2019":
            new_feature_df = new_feature_df.union(feature_df.where(f'tournament = "{test_tournament}" and sample_tag in (0, {sample_tag})')).cache()
    if repeat_num_col != "":
        new_feature_df = new_feature_df\
            .withColumn(repeat_num_col, n_to_array(repeat_num_col))\
            .withColumn(repeat_num_col, F.explode(F.col(repeat_num_col)))
    if mask_cols and mask_rate > 0:
        new_feature_df = new_feature_df \
            .withColumn('rank_tmp', F.expr('row_number() over (partition by content_id order by date)'))
        for mask_col in mask_cols:
            if mask_col.find('cross') > -1:
                new_feature_df = new_feature_df\
                    .withColumn(mask_col, F.expr(f'if({mask_condition} and rank_tmp <= {mask_rate}, empty_{mask_col}, {mask_col})'))
            else:
                new_feature_df = new_feature_df \
                    .withColumn("empty_" + mask_col, mask_array(mask_col)) \
                    .withColumn(mask_col, F.expr(f'if({mask_condition} and rank_tmp <= {mask_rate}, empty_{mask_col}, {mask_col})'))
    if sorting:
        df = new_feature_df\
            .orderBy('date', 'content_id') \
            .toPandas()
    else:
        df = new_feature_df \
            .toPandas()
    return df


def enumerate_tiers(tiers_list):
    if 0 in tiers_list:
        return [tiers_list[0] + tiers_list[1]]
    else:
        return [tiers_list[0] + tiers_list[1] + 1]


def cross_features(dim, *args):
    res = [0 for i in range(dim)]
    idx = 1
    for x in args:
        idx *= x[0] + 1
    res[idx-1] = 1
    return res


def empty_cross_features(dim, *args):
    res = [0 for i in range(dim)]
    idx = 3
    for x in args:
        idx *= x[0] + 1
    res[idx-1] = 1
    return res


def feature_processing(feature_df):
    match_rank_df = feature_df \
        .select('content_id', 'tournament', 'match_stage', 'rand') \
        .distinct() \
        .withColumn("match_rank", F.expr('row_number() over (partition by tournament, match_stage order by rand)')) \
        .select('content_id', 'match_rank') \
        .cache()
    return feature_df \
        .withColumn(repeat_num_col, F.expr(f'if({mask_condition}, {knock_off_repeat_num}, 1)')) \
        .withColumn("hostar_influence_hots_num", F.lit(1)) \
        .withColumn("if_contain_india_team_hots_num", F.lit(2)) \
        .withColumn("if_contain_india_team_hots",
                    if_contain_india_team_hot_vector_udf('if_contain_india_team_hots', 'tournament_type')) \
        .withColumn("if_contain_india_team_hot_vector",
                    generate_hot_vector_udf('if_contain_india_team_hots', 'if_contain_india_team_hots_num')) \
        .join(match_rank_df, 'content_id') \
        .withColumn("knock_rank", F.expr('row_number() over (partition by tournament order by date desc)')) \
        .withColumn("knock_rank", F.expr('if(match_stage not in ("final", "semi-final"), 100, knock_rank)')) \
        .withColumn("knock_rank_hot_vector", F.array(F.col('knock_rank'))) \
        .withColumn("knock_rank_hots_num", F.lit(1)) \
        .withColumn("sample_tag", F.expr('if(tournament="wc2019", if(match_stage="group", if(rand<0.5, 1, 2), if(match_rank<=1, 1, 2)), 0)')) \
        .withColumn("sub_tag", F.array(F.lit(1))) \
        .withColumn("free_tag", F.array(F.lit(0))) \
        .withColumn("sub_free_tag_hots_num", F.lit(1)) \
        .withColumn('sample_repeat_num', F.expr('if(content_id="1440000724", 2, 1)'))\
        .where('date != "2022-08-24" and tournament != "ipl2019"')


def generate_hot_vector(hots, hots_num):
    res = [0 for i in range(hots_num)]
    for hot in hots:
        if hot >= 0:
            res[hot] += 1
    return res


generate_hot_vector_udf = F.udf(generate_hot_vector, ArrayType(IntegerType()))
n_to_array = F.udf(lambda n: [n] * n, ArrayType(IntegerType()))
mask_array = F.udf(lambda l: [0 for i in l], ArrayType(IntegerType()))
enumerate_tiers_udf = F.udf(enumerate_tiers, ArrayType(IntegerType()))
if_contain_india_team_hot_vector_udf = F.udf(lambda x, y: x if y != "national" else [1], ArrayType(IntegerType()))
cross_features_udf = F.udf(cross_features, ArrayType(IntegerType()))
empty_cross_features_udf = F.udf(empty_cross_features, ArrayType(IntegerType()))


hots_num_dic = {
    'if_contain_india_team_hots': 3,
    'match_stage_hots': 4,
    'tournament_type_hots': 3,
    'match_type_hots': 3,
    'vod_type_hots': 2
}
knock_off_repeat_num = 1
repeat_num_col = "knock_off_repeat_num"
mask_condition = 'match_stage in ("final", "semi-final")'
mask_cols = []


def load_train_and_prediction_dataset(DATE, config, if_free_timer):
    base_label_cols = ['active_frees_rate', 'frees_watching_match_rate', "watch_time_per_free_per_match",
                       'active_subscribers_rate', 'subscribers_watching_match_rate', "watch_time_per_subscriber_per_match"]
    if if_free_timer == "":
        label_cols = base_label_cols
    else:
        label_cols = ["watch_time_per_free_per_match_with_free_timer"]
    path_suffix = "/all_features_hots_format" + if_free_timer + xgb_configuration['if_simple_one_hot']
    feature_df = feature_processing(load_data_frame(spark, pipeline_base_path + path_suffix))\
        .cache()
    if config == {}:
        predict_feature_df = feature_processing(reduce(lambda x, y: x.union(y), [load_data_frame(spark, live_ads_inventory_forecasting_complete_feature_path + "/" + tournament
                                                      + "/all_features_hots_format" + xgb_configuration['if_simple_one_hot']) for tournament in xgb_configuration['predict_tournaments_candidate']])\
                                                .withColumn("rand", F.rand(seed=54321))) \
            .withColumn('free_timer', F.lit(1000))\
            .withColumn('request_id', F.lit("0"))\
            .cache()
    else:
        base_path_suffix = "/prediction/all_features_hots_format" + if_free_timer + xgb_configuration['if_simple_one_hot']
        predict_feature_df = feature_processing(load_data_frame(spark, pipeline_base_path + base_path_suffix + f"/cd={DATE}")
                                                .withColumn("rand", F.rand(seed=54321))) \
            .cache()
    if xgb_configuration['prediction_svod_tag'] != "":
        predict_feature_df = predict_feature_df \
            .withColumn("vod_type_hots", F.array(F.lit(1))) \
            .withColumn("vod_type_hot_vector", F.array(F.lit(1))) \
            .cache()
    one_hot_cols = ['tournament_type', 'if_weekend', 'match_time', 'if_holiday', 'venue', 'if_contain_india_team',
                    'match_type', 'tournament_name', 'hostar_influence',
                    'match_stage', 'vod_type']
    multi_hot_cols = ['teams', 'continents', 'teams_tier']
    additional_cols = ["languages", "platforms"]
    if not xgb_configuration['if_hotstar_influence']:
        one_hot_cols.remove('hostar_influence')
    if not xgb_configuration['if_teams']:
        multi_hot_cols.remove('teams')
    if xgb_configuration['if_improve_ties']:
        feature_df = feature_df \
            .withColumn("teams_tier_hot_vector", enumerate_tiers_udf('teams_tier_hots')) \
            .withColumn("teams_tier_hots_num", F.lit(1)) \
            .cache()
        predict_feature_df = predict_feature_df \
            .withColumn("teams_tier_hot_vector", enumerate_tiers_udf('teams_tier_hots')) \
            .withColumn("teams_tier_hots_num", F.lit(1)) \
            .cache()
    mask_features = ['if_contain_india_team']
    if xgb_configuration['if_cross_features']:
        for idx in range(len(xgb_configuration['cross_features'])):
            feature_dim = reduce(lambda x, y: x * y, [hots_num_dic[feature] for feature in xgb_configuration['cross_features'][idx]])
            # print(feature_dim)
            feature_df = feature_df \
                .withColumn(f"cross_features_{idx}_hots_num", F.lit(feature_dim)) \
                .withColumn(f"cross_features_{idx}_hot_vector", cross_features_udf(f"cross_features_{idx}_hots_num", *xgb_configuration['cross_features'][idx])) \
                .withColumn(f"empty_cross_features_{idx}_hot_vector", empty_cross_features_udf(f"cross_features_{idx}_hots_num", *(xgb_configuration['cross_features'][idx][1:]))) \
                .cache()
            predict_feature_df = predict_feature_df \
                .withColumn(f"cross_features_{idx}_hots_num", F.lit(feature_dim)) \
                .withColumn(f"cross_features_{idx}_hot_vector", cross_features_udf(f"cross_features_{idx}_hots_num", *xgb_configuration['cross_features'][idx])) \
                .withColumn(f"empty_cross_features_{idx}_hot_vector", empty_cross_features_udf(f"cross_features_{idx}_hots_num", *(xgb_configuration['cross_features'][idx][1:]))) \
                .cache()
            one_hot_cols = [f'cross_features_{idx}'] + one_hot_cols
            mask_features.append(f'cross_features_{idx}')
            # feature_df.select(f"cross_features_{idx}_hots_num", f"cross_features_{idx}_hot_vector").show(20, False)
    global mask_cols
    mask_cols = [col+"_hot_vector" for col in multi_hot_cols + mask_features]
    feature_cols = [col+"_hot_vector" for col in one_hot_cols+multi_hot_cols]
    if xgb_configuration['if_contains_language_and_platform']:
        feature_cols += [col+"_hot_vector" for col in additional_cols]
    if if_free_timer != "":
        feature_df = feature_df \
            .withColumn("free_timer_hot_vector", F.array(F.col('free_timer'))) \
            .withColumn("free_timer_hots_num", F.lit(1)) \
            .cache()
        predict_feature_df = predict_feature_df \
            .withColumn("free_timer_hot_vector", F.array(F.col('free_timer'))) \
            .withColumn("free_timer_hots_num", F.lit(1)) \
            .withColumn('watch_time_per_free_per_match_with_free_timer', F.lit(1.0)) \
            .cache()
        feature_cols += ["free_timer_hot_vector"]
    if xgb_configuration['prediction_svod_tag'] != "":
        predict_feature_df = predict_feature_df \
            .withColumn("free_timer_hot_vector", F.array(F.lit(xgb_configuration['default_svod_free_timer'])))\
            .cache()
    return feature_df, predict_feature_df, feature_cols, label_cols


def model_prediction(DATE, test_tournaments, feature_df, predict_feature_df, feature_cols, label_cols, mask_tag="", wc2019_test_tag=1, config={}):
    res_dic = {}
    train_error_list2 = {}
    test_error_list2 = {}
    feature_num_cols = [col.replace("_hot_vector", "_hots_num") for col in feature_cols]
    feature_num_col_list = feature_df.select(*feature_num_cols).distinct().collect()
    labeled_tournaments = [item[0] for item in feature_df.select('tournament').distinct().collect()]
    print(label_cols)
    best_setting_dic = {'active_frees_rate': ['reg:squarederror', '93', '0.1', '9'],
                        'frees_watching_match_rate': ['reg:squarederror', '45', '0.05', '11'],
                        'watch_time_per_free_per_match': ['reg:squarederror', '73', '0.05', '3'],
                        'active_subscribers_rate': ['reg:squarederror', '37', '0.2', '9'],
                        'subscribers_watching_match_rate': ['reg:squarederror', '53', '0.05', '9'],
                        'watch_time_per_subscriber_per_match': ['reg:squarederror', '61', '0.1', '3'],
                        'watch_time_per_free_per_match_with_free_timer': ['reg:squarederror', '73', '0.05', '5']}
    for test_tournament in test_tournaments:
        print(test_tournament)
        res_dic[test_tournament] = {}
        if test_tournament not in labeled_tournaments:
            test_feature_df = predict_feature_df
        else:
            test_feature_df = feature_df
        if mask_tag == "":
            train_df = load_dataset(feature_df, test_tournament, wc2019_test_tag=wc2019_test_tag)
            test_df = load_dataset(test_feature_df, test_tournament, wc2019_test_tag=wc2019_test_tag, sorting=True)
        else:
            train_df = load_dataset(feature_df, test_tournament, wc2019_test_tag=wc2019_test_tag,
                                    repeat_num_col="knock_off_repeat_num",
                                    mask_cols=mask_cols, mask_condition=mask_condition,
                                    mask_rate=int(knock_off_repeat_num / 2))
            test_df = load_dataset(feature_df, test_tournament, wc2019_test_tag=wc2019_test_tag, sorting=True,
                                   mask_cols=mask_cols, mask_condition=mask_condition, mask_rate=1)
        print(len(train_df))
        print(len(test_df))
        multi_col_df = []
        for i in range(len(feature_cols)):
            index = [feature_cols[i] + str(j) for j in range(feature_num_col_list[0][i])]
            multi_col_df.append(train_df[feature_cols[i]].apply(pd.Series, index=index))
        X_train = pd.concat(multi_col_df, axis=1)
        multi_col_df = []
        for i in range(len(feature_cols)):
            index = [feature_cols[i] + str(j) for j in range(feature_num_col_list[0][i])]
            multi_col_df.append(test_df[feature_cols[i]].apply(pd.Series, index=index))
        X_test = pd.concat(multi_col_df, axis=1)
        train_error_list2[test_tournament] = []
        test_error_list2[test_tournament] = []
        for label in label_cols:
            # print("")
            if label in xgb_configuration['unvalid_labels']:
                continue
            print(label)
            # for prediction start
            object_method, n_estimators, learning_rate, max_depth = best_setting_dic[label]
            n_estimators = int(n_estimators)
            learning_rate = float(learning_rate)
            max_depth = int(max_depth)
            y_train = train_df[label]
            if xgb_configuration['model'] == "xgb":
                model = XGBRegressor(base_score=0.0, n_estimators=n_estimators, learning_rate=learning_rate,
                                     max_depth=max_depth, objective=object_method, colsample_bytree=1.0,
                                     colsample_bylevel=1.0, colsample_bynode=1.0)
            else:
                model = RandomForestRegressor(n_estimators=n_estimators, max_depth=max_depth)
            if xgb_configuration['sample_weight']:
                model.fit(X_train, y_train, sample_weight=train_df["sample_repeat_num"])
            else:
                model.fit(X_train, y_train)
            y_pred = model.predict(X_test)
            y_test = test_df[label]
            prediction_df = spark.createDataFrame(
                pd.concat([test_df[['date', 'content_id']], y_test, pd.DataFrame(y_pred)], axis=1),
                ['date', 'content_id', 'real_' + label, 'estimated_' + label]) \
                .withColumn('estimated_' + label, F.expr(f'cast({"estimated_" + label} as float)'))
            if config == {}:
                save_data_frame(prediction_df, pipeline_base_path + f"/xgb_prediction{mask_tag}{xgb_configuration['prediction_svod_tag']}/previous_tournaments/label={label}/tournament={test_tournament}/sample_tag={wc2019_test_tag}")
            else:
                save_data_frame(prediction_df, pipeline_base_path + f"/xgb_prediction{mask_tag}{xgb_configuration['prediction_svod_tag']}/future_tournaments/cd={DATE}/label={label}")
            error = metrics.mean_absolute_error(y_test, y_pred)
            y_mean = y_test.mean()
            print(error / y_mean)
            res_dic[test_tournament][label] = error / y_mean
    print(res_dic)


def main(DATE, config={}, free_time_tag=""):
    feature_df, predict_feature_df, feature_cols, label_cols = load_train_and_prediction_dataset(DATE, config, if_free_timer=free_time_tag)
    if config == {}:
        items = [(["wc2019", "wc2021", "ipl2022", "ac2022", "wc2022"], 1),
                 (["wc2019"], 2),
                 (["wc2023"], 1)]
        for item in items:
            model_prediction(DATE, item[0], feature_df, predict_feature_df, feature_cols, label_cols, mask_tag="",
                             wc2019_test_tag=item[1], config=config)
    else:
        test_tournaments = [""]
        # for tournament in config:
        #     test_tournaments.append(tournament['seasonName'].replace(" ", "_").lower())
        print(test_tournaments)
        model_prediction(DATE, test_tournaments, feature_df, predict_feature_df, feature_cols, label_cols, mask_tag="", config=config)


if __name__ == '__main__':
    DATE = sys.argv[1]
    config = load_requests(DATE)
    # main()
    xgb_configuration['prediction_svod_tag'] = ''
    main(DATE, config=config)
    main(DATE, config=config, free_time_tag="_and_free_timer")
    xgb_configuration['prediction_svod_tag'] = '_svod'
    main(DATE, config=config)
    main(DATE, config=config, free_time_tag="_and_free_timer")