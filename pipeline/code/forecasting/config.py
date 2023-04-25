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


unvalid_team_mapping = {
        "sl": "sri lanka",
        "eng": "england",
        "ire": "ireland",
        "dc.": "dc"
}

xgb_configuration = {
    'model': "xgb",
    # 'model': "random_forest",
    'tournament_list': "all",
    # 'tournament_list': "svod",
    # 'tournament_list': "t20",
    # 'mask_tag': "mask_knock_off",
    'predict_tournaments_candidate': ["wc2023"],
    'mask_tag': "",
    # 'sample_weight': False,
    'sample_weight': True,
    'unvalid_labels': ['active_frees_rate', 'active_subscribers_rate'],
    # 'unvalid_labels': [],
    # 'if_contains_language_and_platform': True,
    'if_contains_language_and_platform': False,
    'if_improve_ties': True,
    # 'if_improve_ties': False,
    'if_simple_one_hot': "_and_simple_one_hot",
    # 'if_simple_one_hot': "",
    # 'if_free_timer': "_and_free_timer",
    'if_free_timer': "",
    # 'if_hotstar_influence': False,
    'if_hotstar_influence': True,
    # 'if_teams': False,
    'if_teams': True,
    'if_cross_features': True,
    # 'if_cross_features': False,
    # 'cross_features': [['if_contain_india_team_hot_vector', 'match_stage_hots', 'tournament_type_hots']],
    'cross_features': [['if_contain_india_team_hots', 'match_stage_hots', 'tournament_type_hots'],
                       ['if_contain_india_team_hots', 'match_type_hots', 'tournament_type_hots'],
                       ['if_contain_india_team_hots', 'vod_type_hots', 'tournament_type_hots']],
    'prediction_svod_tag': "",
    'default_svod_free_timer': 5,
    'end_tag': 0
}


duration_configurations = [(210.0, 55.0, 80.0), (210.0, 85.0, 30.0), (210.0, 45.0, 55.0)]
drop_off_rate = 0.85

default_predict_tournament = "wc2023"
sub_pid_did_rate = 0.94
free_pid_did_rate = 1.02