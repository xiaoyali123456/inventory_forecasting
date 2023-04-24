set -exu

DATE="2023-04-20"
CODE="s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/pipeline/code/"
SPARK="spark-submit --deploy-mode client \
    --packages org.apache.hudi:hudi-spark-bundle_2.11:0.9.0 \
    --py-files common.py"

aws s3 sync $(dirname $0) $CODE
aws s3 sync $CODE .

bash test/server.sh &
sleep 5

# preprocess
# python3 fetch_requests.py $DATE
# $SPARK check_new_match.py $DATE

# forecasting
# $SPARK forecasting/active_user_etl_and_predict.py $DATE

# $SPARK forecasting/feature.py $DATE
# $SPARK forecasting/xgb_model.py $DATE # TODO: line 325: 'Path does not exist: s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/pipeline/prediction/all_features_hots_format_and_simple_one_hot/cd=2023-04-21;'
$SPARK forecasting/inventory_prediction.py $DATE