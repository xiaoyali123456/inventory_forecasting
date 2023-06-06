from path import *
from util import *
from config import *


# convert tiers vector-based feature into ordered number according to the tier level
def enumerate_tiers(tiers_list):
    if 0 in tiers_list:
        return [tiers_list[0] + tiers_list[1]]
    else:
        return [tiers_list[0] + tiers_list[1] + 1]


# generate cross features
def generate_cross_features(dim, *args):
    res = [0 for i in range(dim)]
    idx = 1
    for x in args:
        idx *= x[0] + 1
    res[idx-1] = 1
    return res


# generate masked cross features
def generate_masked_cross_features(dim, *args):
    res = [0 for i in range(dim)]
    idx = 3  # masked feature for if_contains_india_team
    for x in args:
        idx *= x[0] + 1
    res[idx-1] = 1
    return res


def generate_hot_vector(hots, hots_num):
    res = [0 for i in range(hots_num)]
    for hot in hots:
        if hot >= 0:
            res[hot] += 1
    return res


def set_ordered_tier_feature(df):
    return df\
        .withColumn("teams_tier_hot_vector", enumerate_tiers_udf('teams_tier_hots')) \
        .withColumn("teams_tier_hots_num", F.lit(1))


def generate_cross_feature(df, idx, feature_dim):
    return df \
        .withColumn(f"cross_features_{idx}_hots_num", F.lit(feature_dim)) \
        .withColumn(f"cross_features_{idx}_hot_vector", generate_cross_features_udf(f"cross_features_{idx}_hots_num", *xgb_configuration['cross_features'][idx])) \
        .withColumn(f"masked_cross_features_{idx}_hot_vector", generate_masked_cross_features_udf(f"cross_features_{idx}_hots_num", *(xgb_configuration['cross_features'][idx][1:])))


def set_svod_feature(df):
    svod_value = 1
    return df \
        .withColumn("vod_type_hots", F.array(F.lit(svod_value))) \
        .withColumn("vod_type_hot_vector", F.array(F.lit(svod_value)))


def convert_vector_unit_features_to_value_unit_features(df, feature_cols, feature_num_col_list):
    multi_col_df_list = []
    for i in range(len(feature_cols)):
        index = [feature_cols[i] + str(j) for j in range(feature_num_col_list[0][i])]
        multi_col_df_list.append(df[feature_cols[i]].apply(pd.Series, index=index))
    return pd.concat(multi_col_df_list, axis=1)


generate_hot_vector_udf = F.udf(generate_hot_vector, ArrayType(IntegerType()))
n_to_array = F.udf(lambda n: [n] * n, ArrayType(IntegerType()))
mask_array = F.udf(lambda l: [0 for i in l], ArrayType(IntegerType()))
enumerate_tiers_udf = F.udf(enumerate_tiers, ArrayType(IntegerType()))
if_contain_india_team_hot_vector_udf = F.udf(lambda x, y: x if y != "national" else [1], ArrayType(IntegerType()))
generate_cross_features_udf = F.udf(generate_cross_features, ArrayType(IntegerType()))
generate_masked_cross_features_udf = F.udf(generate_masked_cross_features, ArrayType(IntegerType()))


feature_candidate_num_dic = {
    'if_contain_india_team_hots': 3,  # 0 for not containing india, 1 for containing india, and 2 for masked feature
    'match_stage_hots': 4,
    'tournament_type_hots': 3,
    'match_type_hots': 3,
    'vod_type_hots': 2
}
knock_off_repeat_num = 1
repeat_num_col = "knock_off_repeat_num"
mask_condition = 'match_stage in ("final", "semi-final")'
mask_cols = []


def feature_processing(df, if_contains_free_timer_feature=False, if_make_matches_svod=False):
    feature_cols = [col + "_hot_vector" for col in one_hot_cols + multi_hot_cols + numerical_cols]
    df = set_ordered_tier_feature(df)  # convert tier feature from a multi-hot vector to a ordered number
    if xgb_configuration['if_contains_cross_features']:
        # generate cross features
        for idx in range(len(xgb_configuration['cross_features'])):
            feature_dim = reduce(lambda x, y: x * y, [feature_candidate_num_dic[feature] for feature in
                                                      xgb_configuration['cross_features'][idx]])
            df = generate_cross_feature(df, idx, feature_dim)
            feature_cols.append(f'cross_features_{idx}_hot_vector')
    if not if_contains_free_timer_feature:
        feature_cols.remove("free_timer_hot_vector")
    if if_make_matches_svod:
        df = set_svod_feature(df)\
            .withColumn("free_timer_hot_vector", F.array(F.lit(xgb_configuration['default_svod_free_timer'])))  # set vod type and free timer features for svod matches
    return df.withColumn('sample_weight', F.expr(f'if(content_id="{important_content_id}", {important_content_weight}, 1)')), feature_cols


def data_split(df, split_rate=0.9):
    df = df.withColumn("rand", F.rand(seed=54321))
    train_df = df.where(f"rand <= {split_rate}")
    val_df = df.where(f"rand > {split_rate}")
    return train_df, val_df


def feature_processing_and_dataset_split(dataset_path, if_contains_free_timer_feature=False, if_make_matches_svod=False):
    all_df = load_data_frame(spark, dataset_path) \
        .where(f'date != "{invalid_match_date}" and tournament != "{invalid_tournament}"')
    all_df, feature_cols = feature_processing(all_df, if_contains_free_timer_feature=if_contains_free_timer_feature, if_make_matches_svod=if_make_matches_svod)
    train_df, val_df = data_split(all_df)
    return all_df, train_df, val_df, feature_cols


def model_train_and_test(train_df, feature_cols, test_df, label_cols, if_validation=False, if_make_matches_svod=False, DATE=""):
    feature_num_cols = [col.replace("_hot_vector", "_hots_num") for col in feature_cols]
    feature_num_col_list = train_df.select(*feature_num_cols).distinct().collect()
    print(feature_num_col_list)
    # explode vector-based features of train/test dataset into multiple one-single-value features
    train_df = train_df.toPandas()
    test_df = test_df.toPandas()
    train_feature_df = convert_vector_unit_features_to_value_unit_features(train_df, feature_cols=feature_cols,
                                                                           feature_num_col_list=feature_num_col_list)
    test_feature_df = convert_vector_unit_features_to_value_unit_features(test_df, feature_cols=feature_cols,
                                                                          feature_num_col_list=feature_num_col_list)
    for label in label_cols:
        object_method, n_estimators, learning_rate, max_depth = xgb_hyper_parameter_dic[label]
        train_label_df = train_df[label]
        model = XGBRegressor(base_score=0.0, n_estimators=n_estimators, learning_rate=learning_rate,
                             max_depth=max_depth, objective=object_method)
        # print(train_feature_df)
        # print(train_label_df)
        model.fit(train_feature_df, train_label_df, sample_weight=train_df["sample_weight"])
        # model.fit(train_feature_df, train_label_df)
        test_prediction_df = model.predict(test_feature_df)
        test_label_df = test_df[label]
        prediction_df = spark.createDataFrame(
            pd.concat([test_df[['date', 'content_id']], test_label_df, pd.DataFrame(test_prediction_df)], axis=1),
            ['date', 'content_id', 'real_' + label, 'estimated_' + label]) \
            .withColumn('estimated_' + label, F.expr(f'cast({"estimated_" + label} as float)'))
        if if_validation:
            label_mean = test_label_df.mean()
            error = metrics.mean_absolute_error(test_label_df, test_prediction_df)
            slack_notification(topic=slack_notification_topic, region=region, message=f"error of {label}: {error/label_mean}")
        else:
            path_suffix = "_svod" if if_make_matches_svod else ""
            save_data_frame(prediction_df, pipeline_base_path + f"/xgb_prediction{path_suffix}/future_tournaments/cd={DATE}/label={label}")


def model_train_and_validation(label_cols, dataset_path):
    all_df, train_df, val_df, feature_cols = feature_processing_and_dataset_split(dataset_path=dataset_path)
    model_train_and_test(train_df=train_df, feature_cols=feature_cols, test_df=val_df, label_cols=label_cols, if_validation=True)
    return all_df, feature_cols


def model_train_and_validation_overall():
    base_df, base_feature_cols = model_train_and_validation(label_cols=[free_rate_label, sub_rate_label, sub_wt_label],
                                                            dataset_path=pipeline_base_path + "/all_features_hots_format_and_simple_one_hot")
    free_wt_df, free_wt_feature_cols = model_train_and_validation(label_cols=[free_wt_label],
                                                                  dataset_path=pipeline_base_path + "/all_features_hots_format_and_free_timer_and_simple_one_hot")
    return base_df, base_feature_cols, free_wt_df, free_wt_feature_cols


def model_train_and_prediction_overall(DATE, base_train_df, base_feature_cols, free_wt_train_df, free_wt_feature_cols):
    raw_prediction_df = load_data_frame(spark, pipeline_base_path + f"/prediction/all_features_hots_format_and_simple_one_hot/cd={DATE}").cache()
    # predicting free_rate_label(mixed-vod), sub_rate_label, sub_wt_label
    base_prediction_df, _ = feature_processing(raw_prediction_df)
    model_train_and_test(train_df=base_train_df, feature_cols=base_feature_cols, test_df=base_prediction_df, label_cols=[free_rate_label, sub_rate_label, sub_wt_label],
                         DATE=DATE)
    # predicting free_rate_label(svod)
    svod_prediction_df, _ = feature_processing(raw_prediction_df, if_make_matches_svod=True)
    model_train_and_test(train_df=base_train_df, feature_cols=base_feature_cols, test_df=svod_prediction_df, label_cols=[free_rate_label],
                         if_make_matches_svod=True, DATE=DATE)
    # predicting free_wt_label
    free_wt_prediction_df, _ = feature_processing(raw_prediction_df, if_contains_free_timer_feature=True)
    model_train_and_test(train_df=free_wt_train_df, feature_cols=free_wt_feature_cols, test_df=free_wt_prediction_df, label_cols=[free_wt_label],
                         DATE=DATE)


def main(DATE, request):
    """
    Label	            Features	        Train samples	Predicting avod matches
    free_rate_label	    14	                408	            combine mixed-vod prediction with svod prediction
    free_wt_label	    14 + free_timer	    408+45	        just predicting
    sub_rate_label	    14	                408	            just predicting
    sub_wt_label 	    14	                408	            just predicting
    """
    base_df, base_feature_cols, free_wt_df, free_wt_feature_cols = model_train_and_validation_overall()
    if request != {}:
        model_train_and_prediction_overall(DATE, base_df, base_feature_cols, free_wt_df, free_wt_feature_cols)


if __name__ == '__main__':
    DATE = sys.argv[1]
    request = load_requests(DATE)
    # model train and test for matches
    # xgb_configuration['prediction_svod_tag'] = ''
    # main(DATE, config=config, free_timer_tag="")
    # main(DATE, config=config, free_timer_tag="_and_free_timer")
    # # model train and test for matches assuming svod type
    # xgb_configuration['prediction_svod_tag'] = '_svod'
    # main(DATE, config=config, free_timer_tag="")
    main(DATE, request)
