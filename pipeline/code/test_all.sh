set -exu

DATE="2023-04-18"
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

$SPARK forecasting/feature.py $DATE