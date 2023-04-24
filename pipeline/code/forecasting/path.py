play_out_log_input_path = "s3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test"
watchAggregatedInputPath = "s3://hotstar-dp-datalake-processed-us-east-1-prod/aggregates/watched_video_daily_aggregates_ist_v4"
active_user_num_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_v3/truth/"
live_ads_inventory_forecasting_root_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting"
live_ads_inventory_forecasting_complete_feature_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/complete_features"
pipeline_base_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/pipeline"
dau_prediction_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_v3/forecast/"

concurrency_root_path = "s3://hotstar-dp-datalake-processed-us-east-1-prod/hive_internal_database/concurrency.db/"
ssai_concurrency_path = f"{concurrency_root_path}/users_by_live_sports_content_by_ssai"
match_meta_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/ads_crash/match_meta"
viewAggregatedInputPath = "s3://hotstar-dp-datalake-processed-us-east-1-prod/aggregates/viewed_page_daily_aggregates_ist_v2"

concurrency_root_path = "s3://hotstar-dp-datalake-processed-us-east-1-prod/hive_internal_database/concurrency.db/"
impression_path = "s3://hotstar-data-lake-northvirginia/data/source/campaignTracker/parquet_bifrost/impression_events"
watch_video_path = "s3://hotstar-dp-datalake-processed-us-east-1-prod/events/watched_video/"
watch_video_sampled_path = "s3://hotstar-ads-ml-us-east-1-prod/data_exploration/data/data_backup/watched_video/"
play_out_log_v2_input_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/playout_v2/cd='
play_out_log_v3_input_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/playout_v3/'
play_out_log_original_input_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/playout_original/cd="
playout_log_path_suffix = "/playout_log"

content_id_col = "Content ID"
start_time_col = "Start Time"
end_time_col = "End Time"
break_duration_col = "Delivered Time"
content_language_col = "Language"
platform_col = "Platform"
tenant_col = "Tenant"
creative_id_col = "Creative ID"
break_id_col = "Break ID"
playout_id_col = "Playout ID"
creative_path_col = "Creative Path"
content_id_col2 = "_c4"
start_time_col2 = "_c2"
end_time_col2 = "_c13"
break_duration_col2 = "_c15"
content_language_col2 = "_c5"
platform_col2 = "_c8"
tenant_col2 = "_c6"
creative_id_col2 = "_c9"
break_id_col2 = "_c10"
playout_id_col2 = "_c3"
creative_path_col2 = "_c11"
content_language_col3 = "_c1"

