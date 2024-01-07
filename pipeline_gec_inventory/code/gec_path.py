# Path definition
from typing import Any

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
ALL_MATCH_TABLE_PATH = f"{PIPELINE_BASE_PATH}/match_table/all"
TRAIN_MATCH_TABLE_PATH = f"{PIPELINE_BASE_PATH}/match_table/train"
PREDICTION_MATCH_TABLE_PATH = f"{PIPELINE_BASE_PATH}/match_table/prediction"

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
# HOLIDAYS_FEATURE_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/holidays/latest/holidays_v4.csv'
HOLIDAYS_FEATURE_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/holidays/latest/holidays_v5.csv'
SUB_HOLIDAYS_FEATURE_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/holidays/latest/holidays_v5_sub.csv'
FREE_HOLIDAYS_FEATURE_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/holidays/latest/holidays_v5_free.csv'

# sampling
SAMPLING_ROOT_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_v2/'
INVENTORY_SAMPLING_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_v2/inventory/'
CUSTOM_COHORT_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_v2/custom_chort/'
AD_TIME_SAMPLING_OLD_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_v2/ad_time_old/'
REACH_SAMPLING_OLD_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_v2/reach_old/'
AD_TIME_SAMPLING_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_v2/ad_time/'
REACH_SAMPLING_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_v2/reach/'
PLAYOUT_PATH = 's3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test/'
WV_S3_BACKUP = 's3://hotstar-ads-ml-us-east-1-prod/data_exploration/data/data_backup/watched_video/'
WV_TABLE = 'data_lake.watched_video'
PREROLL_INVENTORY_PATH = 's3://hotstar-ads-targeting-us-east-1-prod/trackers/shifu_ad_events/ad_inventory/'
PREROLL_INVENTORY_AGG_PATH = 's3://hotstar-ads-targeting-us-east-1-prod/trackers/shifu_reporting/aggregates/hourly/ad_inventory/'
TMP_WATCHED_VIDEO_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_v2/watched_video_tmp/'
# preroll
# PREROLL_SAMPLING_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_preroll/inventory/'
PREROLL_SAMPLING_ROOT_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_preroll/'
PREROLL_SAMPLING_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_preroll/etl_result/'
PREROLL_INVENTORY_RATIO_RESULT_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_preroll/ratio/inventory/'
PREROLL_REACH_RATIO_RESULT_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_preroll/ratio/reach/'

# total inventory
MODEL_VERSION = ""
# MODEL_VERSION = "_epoch_60"
# MODEL_VERSION = "_incremental"
TOTAL_INVENTORY_PREDICTION_PATH = f'{PIPELINE_BASE_PATH}/inventory_prediction{MODEL_VERSION}/future_tournaments/'
FINAL_INVENTORY_PREDICTION_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/final/inventory/'
FINAL_REACH_PREDICTION_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/final/reach/'
FINAL_ALL_PREDICTION_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/final/all/'
FINAL_ALL_PREDICTION_TOURNAMENT_PARTITION_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/final/all_tournament_partition/'
FINAL_ALL_PREROLL_PREDICTION_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/final/preroll_all/'
FINAL_ALL_PREROLL_PREDICTION_TOURNAMENT_PARTITION_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/final/preroll_all_tournament_partition/'
GROWTH_PREDICITON_PATH = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/pipeline/label/growth_prediction/"
ML_STATIC_MODEL_PREDICITON_FOR_AC2023_PATH = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/pipeline/label/ml_static_model_prediction/"
ML_STATIC_MODEL_PREDICITON_FOR_CWC2023_PATH = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/pipeline/label/ml_static_model_prediction_for_cwc2023/"
METRICS_PATH = f"s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/pipeline/label/metrics{MODEL_VERSION}"


CUSTOM_AUDIENCE_COL = "customAudienceResponses"


# for gec
INVENTORY_S3_ROOT_PATH: Any = "s3://hotstar-ads-targeting-us-east-1-prod/trackers/shifu_ad_events/ad_inventory"
EPISODE_TABLE = "in_cms.episode_update_s3"
MOVIE_TABLE = "in_cms.movie_update_s3"
CLIP_TABLE = "in_cms.clip_update_s3"
MATCH_TABLE = "in_cms.match_update_s3"

ROOT_PATH = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/gec_inventory_forecasting"
BACKUP_PATH = f"{ROOT_PATH}/backup"
CMS_DATA_PATH = f"{ROOT_PATH}/cms_data"
CMS_MATCH_DATA_PATH = f"{ROOT_PATH}/cms_match_data"
SAMPLING_DATA_PATH = f"{ROOT_PATH}/sampling_data"
SAMPLING_DATA_NEW_PATH = f"{ROOT_PATH}/sampling_data_new"
SAMPLING_DATA_SUMMARY_PATH = f"{ROOT_PATH}/sampling_data_summary"
VOD_SAMPLING_DATA_PREDICTION_PATH = f"{ROOT_PATH}/vod_sampling_data_prediction"
VOD_SAMPLING_DATA_PREDICTION_CSV_PATH = f"{ROOT_PATH}/vod_sampling_data_prediction_csv"
VOD_SAMPLING_DATA_PREDICTION_PARQUET_PATH = f"{ROOT_PATH}/vod_sampling_data_prediction_parquet"
VOD_SAMPLING_PREDICTION_ERROR_PATH = f"{ROOT_PATH}/vod_sampling_prediction_error"
VOD_SAMPLING_ERROR_PATH = f"{ROOT_PATH}/vod_sampling_error"

ORIGINAL_DATA_FCAP_HISTOGRAM_PATH = f"{ROOT_PATH}/original_data_fcap_histogram"
SAMPLED_DATA_FCAP_HISTOGRAM_PATH = f"{ROOT_PATH}/sampled_data_fcap_histogram"
USER_ID_BACKUP_PATH = f"{ROOT_PATH}/user_id_backup"

GEC_INVENTORY_BY_CD_PATH = f"{ROOT_PATH}/prophet/gec_inventory_by_cd"
GEC_INVENTORY_BY_AD_PLACEMENT_PATH = f"{ROOT_PATH}/prophet/gec_inventory_by_ad_placement"

INVENTORY_NUMBER_PATH = "s3://hotstar-ads-ml-us-east-1-prod/inventory_forecast/gec/ingest/inventoryNumber"


