# preprocess
set -exu

DATE="2023-04-18"
CODE="s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/pipeline/code/"

aws s3 sync $CODE .

bash test/run.sh &
sleep 3

SPARK="spark-submit --deploy-mode client \
    --packages org.apache.hudi:hudi-spark-bundle_2.11:0.9.0"

python3 fetch_requests.py $DATE
$SPARK check_new_match.py $DATE
