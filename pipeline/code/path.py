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


# preprocess
PREPROCESSED_INPUT_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/inventory_forecast_input/'
REQUESTS_PATH_TEMPL = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/inventory_requests/cd=%s/requests.json'

MATCH_CMS_PATH_TEMPL = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/cms_match/cd=%s/'
# BOOKING_TOOL_URL = 'http://localhost:4321/'
BOOKING_TOOL_URL = 'http://adtech-inventory-booking-service-alb-0-int.internal.sgp.hotstar.com/'

# DAU
DAU_TRUTH_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_v3/truth/'
DAU_FORECAST_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_v3/forecast/'
DAU_COMBINE_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_v3/combine/'
DAU_TABLE = 'data_warehouse.watched_video_daily_aggregates_ist'
# HOLIDAYS_FEATURE_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/holidays/latest/holidays_v2_4.csv'
HOLIDAYS_FEATURE_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/holidays/latest/holidays_v4.csv'

# sampling
INVENTORY_SAMPLING_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_v2/inventory/'
CUSTOM_COHORT_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_v2/custom_chort/'
AD_TIME_SAMPLING_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_v2/ad_time/'
REACH_SAMPLING_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_v2/reach/'
PLAYOUT_PATH = 's3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test/'
WV_S3_BACKUP = 's3://hotstar-ads-ml-us-east-1-prod/data_exploration/data/data_backup/watched_video/'
WV_TABLE = 'data_lake.watched_video'
PREROLL_INVENTORY_PATH = 's3://hotstar-ads-targeting-us-east-1-prod/trackers/shifu_ad_events/ad_inventory/'
PREROLL_INVENTORY_AGG_PATH = 's3://hotstar-ads-targeting-us-east-1-prod/trackers/shifu_reporting/aggregates/hourly/ad_inventory/'
TMP_WATCHED_VIDEO_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_v2/watched_video_tmp/'
# preroll
PREROLL_SAMPLING_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_preroll/inventory/'
PREROLL_RATIO_RESULT_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_preroll/ratio/'


# total inventory
TOTAL_INVENTORY_PREDICTION_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/pipeline/inventory_prediction/future_tournaments/'
FINAL_INVENTORY_PREDICTION_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/final/inventory/'
FINAL_REACH_PREDICTION_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/final/reach/'
FINAL_ALL_PREDICTION_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/final/all/'
FINAL_ALL_PREDICTION_TOURNAMENT_PARTITION_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/final/all_tournament_partition/'


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
