for DATE in 2023-02-11 2023-03-17 2023-03-19 2023-03-22 2023-02-12 2023-03-18 2023-03-20 2023-03-23
do
  TASK="inventory"
  aws s3 sync s3://hotstar-ads-targeting-us-east-1-prod/trackers/shifu_ad_events/ad_"${TASK}"/cd="${DATE}" s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/preroll/"${TASK}"_data/raw_data/"${DATE}"
#  TASK="impression"
#  aws s3 sync s3://hotstar-ads-targeting-us-east-1-prod/trackers/shifu_ad_events/ad_"${TASK}"/cd="${DATE}" s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/preroll/"${TASK}"_data/raw_data/"${DATE}"
done