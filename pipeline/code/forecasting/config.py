continent_dic = {
    'australia': 'OC',
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
    'uae': 'AS'
}

tiers_dic = {
    'australia': 'tier1',
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


invalid_team_mapping = {
        "sl": "sri lanka",
        "eng": "england",
        "ire": "ireland",
        "dc.": "dc"
}

xgb_configuration = {
    'model': "xgb",
    # 'model': "random_forest",
    # 'mask_tag': "mask_knock_off",
    'predict_tournaments_candidate': ["wc2023"],
    'mask_tag': "",
    # 'sample_weight': False,
    'sample_weight': True,
    # 'unvalid_labels': [],
    'if_improve_ties': True,
    # 'if_improve_ties': False,
    'simple_one_hot_suffix': "_and_simple_one_hot",
    # 'simple_one_hot_suffix': "",
    # 'if_free_timer': "_and_free_timer",
    'if_free_timer': "",
    'if_contains_cross_features': True,
    # 'if_contains_cross_features': False,
    # 'cross_features': [['if_contain_india_team_hot_vector', 'match_stage_hots', 'tournament_type_hots']],
    'cross_features': [['if_contain_india_team_hots', 'match_stage_hots', 'tournament_type_hots'],
                       ['if_contain_india_team_hots', 'match_type_hots', 'tournament_type_hots'],
                       ['if_contain_india_team_hots', 'vod_type_hots', 'tournament_type_hots']],
    'prediction_svod_tag': "",
    'default_svod_free_timer': 5,
    'end_tag': 0
}

free_rate_label = "frees_watching_match_rate"
free_wt_label = "watch_time_per_free_per_match_with_free_timer"
sub_rate_label = "subscribers_watching_match_rate"
sub_wt_label = "watch_time_per_subscriber_per_match"

# hyper-parameter setting from grid search, with [object_method, n_estimators, learning_rate, max_depth] format
xgb_hyper_parameter_dic = {free_rate_label: ['reg:squarederror', 45, 0.05, 11],
                           free_wt_label: ['reg:squarederror', 73, 0.05, 5],
                           sub_rate_label: ['reg:squarederror', 53, 0.05, 9],
                           sub_wt_label: ['reg:squarederror', 61, 0.1, 3]}

one_hot_cols = ['tournament_type', 'if_weekend', 'match_time', 'if_holiday', 'venue', 'if_contain_india_team',
                'match_type', 'tournament_name', 'match_stage', 'vod_type']
multi_hot_cols = ['teams', 'continents', 'teams_tier']
numerical_cols = ['hotstar_influence', 'free_timer']
additional_cols = ["languages", "platforms"]


# duration_configurations = [(210.0, 55.0, 80.0), (210.0, 85.0, 30.0), (210.0, 45.0, 55.0)]
match_configuration = (210.0, 85.0, 30.0)  # total_match_duration_in_minutes, number_of_ad_breaks, average_length_of_a_break_in_seconds
drop_off_rate = 0.85

default_predict_tournament = "wc2023"
sub_pid_did_rate = 0.94
free_pid_did_rate = 1.02

invalid_match_date = '2022-08-24'
invalid_tournament = 'ipl2019'
important_content_id = "1440000724"
important_content_weight = 2
meaningless_request_id = "0"
jio_user_rate_of_wc2019 = 0.75
slack_notification_topic = "arn:aws:sns:us-east-1:253474845919:sirius-notification"
region = "us-east-1"