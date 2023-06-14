#!/bin/bash
#set -exu

#DATE=$1
#CODE=$2
DATE="2023-06-14"
CODE="s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/pipeline/code/"

source activate pytorch

aws s3 sync $CODE .

# shellcheck disable=SC2164
cd forecasting/rate_and_wt_prediction
python3 main.py $DATE


