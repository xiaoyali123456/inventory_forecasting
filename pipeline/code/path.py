# Path definition

PLAY_OUT_LOG_INPUT_PATH = "s3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test"
WATCH_AGGREGATED_INPUT_PATH = "s3://hotstar-dp-datalake-processed-us-east-1-prod/aggregates/watched_video_daily_aggregates_ist_v4"
ACTIVE_USER_NUM_PATH = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_v3/truth/"
LIVE_ADS_INVENTORY_FORECASTING_ROOT_PATH = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting"
LIVE_ADS_INVENTORY_FORECASTING_COMPLETE_FEATURE_PATH = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/complete_features"
PIPELINE_BASE_PATH = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/pipeline"
TRAINING_DATA_PATH = f"{PIPELINE_BASE_PATH}/all_features_hots_format_full_avod_and_simple_one_hot_overall"
DVV_PREDICTION_PATH = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_v3/forecast/"
DVV_TRUTH_PATH = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_v3/truth/"
DVV_COMBINE_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_v3/combine/'
AVG_DVV_PATH = f"{PIPELINE_BASE_PATH}/avg_dau"
PIPELINE_DATA_TMP_PATH = f"{PIPELINE_BASE_PATH}/dataset/tmp"
TRAIN_MATCH_TABLE_PATH = f"{PIPELINE_BASE_PATH}/match_table/train"
PREDICTION_MATCH_TABLE_PATH = f"{PIPELINE_BASE_PATH}/match_table/prediction"
INVENTORY_FORECAST_REQUEST_PATH = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/inventory_forecast_input"

VIEW_AGGREGATED_INPUT_PATH = "s3://hotstar-dp-datalake-processed-us-east-1-prod/aggregates/viewed_page_daily_aggregates_ist_v2"

AGGR_SHIFU_INVENTORY_PATH = "s3://hotstar-ads-targeting-us-east-1-prod/trackers/shifu_reporting/aggregates/hourly/ad_inventory"

IMPRESSION_PATH = "s3://hotstar-data-lake-northvirginia/data/source/campaignTracker/parquet_bifrost/impression_events"
WATCH_VIDEO_PATH = "s3://hotstar-dp-datalake-processed-us-east-1-prod/events/watched_video/"
WATCH_VIDEO_SAMPLED_PATH = "s3://hotstar-ads-ml-us-east-1-prod/data_exploration/data/data_backup/watched_video/"
PLAY_OUT_LOG_V2_INPUT_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/playout_v2/cd='
PLAY_OUT_LOG_V3_INPUT_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/playout_v3/'
PLAY_OUT_LOG_ORIGINAL_INPUT_PATH = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/playout_original/cd="
PLAYOUT_LOG_PATH_SUFFIX = "/playout_log"

CONTENT_ID_COL = "Content ID"
START_TIME_COL = "Start Time"
END_TIME_COL = "End Time"
BREAK_DURATION_COL = "Delivered Time"
CONTENT_LANGUAGE_COL = "Language"
PLATFORM_COL = "Platform"
TENANT_COL = "Tenant"
CREATIVE_ID_COL = "Creative ID"
BREAK_ID_COL = "Break ID"
PLAYOUT_ID_COL = "Playout ID"
CREATIVE_PATH_COL = "Creative Path"
CONTENT_ID_COL2 = "_c4"
START_TIME_COL2 = "_c2"
END_TIME_COL2 = "_c13"
BREAK_DURATION_COL2 = "_c15"
CONTENT_LANGUAGE_COL2 = "_c5"
PLATFORM_COL2 = "_c8"
TENANT_COL2 = "_c6"
CREATIVE_ID_COL2 = "_c9"
BREAK_ID_COL2 = "_c10"
PLAYOUT_ID_COL2 = "_c3"
CREATIVE_PATH_COL2 = "_c11"
CONTENT_LANGUAGE_COL3 = "_c1"

