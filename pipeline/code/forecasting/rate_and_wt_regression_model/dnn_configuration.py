# dnn configuration

PIPELINE_BASE_PATH = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/pipeline"
TRAINING_DATA_PATH = f"{PIPELINE_BASE_PATH}/all_features_hots_format_full_avod_and_simple_one_hot_overall_with_reach_rate"
PREDICTION_FEATURE_PATH = f"{PIPELINE_BASE_PATH}/prediction/all_features_hots_format"
TRAIN_MATCH_TABLE_PATH = f"{PIPELINE_BASE_PATH}/match_table/train"
PREDICTION_MATCH_TABLE_PATH = f"{PIPELINE_BASE_PATH}/match_table/prediction"

SLACK_NOTIFICATION_TOPIC = "arn:aws:sns:us-east-1:253474845919:sirius-notification"
REGION = "us-east-1"

FREE_RATE_LABEL = "frees_watching_match_rate"
FREE_WT_LABEL = "watch_time_per_free_per_match"
SUB_RATE_LABEL = "subscribers_watching_match_rate"
SUB_WT_LABEL = "watch_time_per_subscriber_per_match"
REACH_RATE_LABEL = "reach_rate"
PREROLL_SUB_SESSIONS = "preroll_sub_sessions"
PREROLL_FREE_SESSIONS = "preroll_free_sessions"
LABEL_LIST = [FREE_RATE_LABEL, FREE_WT_LABEL, SUB_RATE_LABEL, SUB_WT_LABEL, REACH_RATE_LABEL,
              PREROLL_SUB_SESSIONS, PREROLL_FREE_SESSIONS]


UNKNOWN_TOKEN = "<unk>"
DEFAULT_CONTINENT = "AS"


DNN_CONFIGURATION = {
    'used_features': [
        'vod_type',
        'match_stage',
        'tournament_name',
        'match_type',
        'if_contain_india_team',
        'if_holiday',
        'match_time',
        'if_weekend',
        'tournament_type',
        'teams',
        'continents',
        'teams_tier'
    ],
    # 'train_batch_size': 16,
    'train_batch_size': 32,
    'test_batch_size': 64,
    # 'epoch_num': 40,
    'epoch_num': 20,
    'lr': 5e-3,
    'weight_decay': 1e-3,
    'embedding_table_size': 100,
    # 'train_batch_size': 64,
    # 'test_batch_size': 64,
    # 'epoch_num': 100,
    # 'lr': 0.01,
    # 'weight_decay': 0,
    # 'embedding_table_size': 32,
    # 'embedding_dim': 6,
    'embedding_dim': 10,
    'mlp_layer_sizes': [64, 64]
}

HUBER_LOSS_PARAMETER_DIC = {
    FREE_RATE_LABEL: 0.1,
    FREE_WT_LABEL: 1,
    SUB_RATE_LABEL: 0.1,
    SUB_WT_LABEL: 1,
    REACH_RATE_LABEL: 0.1,
    PREROLL_FREE_SESSIONS: 1,
    PREROLL_SUB_SESSIONS: 1
}
