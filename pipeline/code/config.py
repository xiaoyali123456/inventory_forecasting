# Forecasting module configuration

CONTINENT_DIC = {
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
    'uae': 'AS',
    'nepal': 'AS'
}

TIERS_DIC = {
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

INVALID_TEAM_MAPPING = {
        "sl": "sri lanka",
        "eng": "england",
        "ire": "ireland",
        "dc.": "dc"
}
UNKNOWN_TOKEN = "<unk>"
UNKNOWN_TOKEN2 = "unknown"
DEFAULT_CONTINENT = "AS"


FREE_RATE_LABEL = "frees_watching_match_rate"
FREE_WT_LABEL = "watch_time_per_free_per_match"
SUB_RATE_LABEL = "subscribers_watching_match_rate"
SUB_WT_LABEL = "watch_time_per_subscriber_per_match"
PREROLL_SUB_SESSIONS = "preroll_sub_sessions"
PREROLL_FREE_SESSIONS = "preroll_free_sessions"


CONTEXT_COLS = ["date", "tournament", "content_id"]
FEATURE_COLS = ["vod_type", "match_stage", "tournament_name", "match_type",
                "if_contain_india_team", "if_holiday", "match_time", "if_weekend",
                "tournament_type", "teams", "continents", "teams_tier", "free_timer"]
LABEL_COLS = ["frees_watching_match_rate", "watch_time_per_free_per_match",
              "subscribers_watching_match_rate", "watch_time_per_subscriber_per_match",
              "reach_rate", "total_reach", "total_inventory",
              "total_frees_number", "match_active_free_num",
              "total_subscribers_number", "match_active_sub_num",
              "preroll_sub_sessions", "preroll_free_sessions",
              "preroll_sub_inventory", "preroll_free_inventory"]
MATCH_TABLE_COLS = CONTEXT_COLS + FEATURE_COLS + LABEL_COLS

ARRAY_FEATURE_COLS = ["teams", "continents", "teams_tier"]


# duration_configurations = [(210.0, 55.0, 80.0), (210.0, 85.0, 30.0), (210.0, 45.0, 55.0)]
MATCH_CONFIGURATION = (210.0, 85.0, 30.0)  # total_match_duration_in_minutes, number_of_ad_breaks, average_length_of_a_break_in_seconds
RETENTION_RATE = 0.85
SUB_PID_DID_RATE = 0.94
FREE_PID_DID_RATE = 1.02


SLACK_NOTIFICATION_TOPIC = "arn:aws:sns:us-east-1:253474845919:sirius-notification"
REGION = "us-east-1"


FOCAL_TOURNAMENTS = [
    "ipl",
    "world cup",
    "asia cup",
    "cricket world cup",
    "sri lanka tour of pakistan",
    "west indies tour india",
    "india tour of new zealand",
    "england tour of india",
    "west indies tour of india",
    "south africa tour of india",
    "sri lanka tour of india",
    "new zealand tour of india",
    "australia tour of india",
]