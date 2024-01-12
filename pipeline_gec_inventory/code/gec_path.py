from gec_config import *

INVENTORY_S3_ROOT_PATH = "s3://hotstar-ads-targeting-us-east-1-prod/trackers/shifu_ad_events/ad_inventory"
EPISODE_TABLE = "in_cms.episode_update_s3"
MOVIE_TABLE = "in_cms.movie_update_s3"
CLIP_TABLE = "in_cms.clip_update_s3"
MATCH_TABLE = "in_cms.match_update_s3"

ROOT_PATH = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/gec_inventory_forecasting"
BACKUP_PATH = f"{ROOT_PATH}/backup"
CMS_DATA_PATH = f"{ROOT_PATH}/cms_data"
SAMPLING_DATA_ALL_ADPLACEMENT_PATH = f"{ROOT_PATH}/sampling_data_sample_rate_100"
SAMPLING_DATA_SUMMARY_PATH = f"{ROOT_PATH}/sampling_data_summary"
VOD_SAMPLING_DATA_PREDICTION_PARQUET_PATH = f"{ROOT_PATH}/vod_sampling_data_prediction_parquet_sample_rate_{VOD_SAMPLE_BUCKET}"

HOLIDAYS_FEATURE_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/holidays/latest/holidays_v5_sub.csv'
GEC_INVENTORY_BY_CD_PATH = f"{ROOT_PATH}/prophet/gec_inventory_by_cd"
GEC_INVENTORY_BY_AD_PLACEMENT_PATH = f"{ROOT_PATH}/prophet/gec_inventory_by_ad_placement"
GEC_INVENTORY_PREDICTION_RESULT_PATH = f"{ROOT_PATH}/prophet/predicted"
GEC_INVENTORY_PREDICTION_REPORT_PATH = f"{ROOT_PATH}/prophet/report"
GEC_INVENTORY_NUMBER_PATH = f"{ROOT_PATH}/ingest/inventoryNumber"

VOD_BITMAP_PICKLE_PATH = f"{ROOT_PATH}/vod_sampling_bitmap_data_{VOD_SAMPLE_BUCKET}/"
VOD_BITMAP_JSON_PATH = f"{ROOT_PATH}/vod_sampling_bitmap_json_data_{VOD_SAMPLE_BUCKET}/"



