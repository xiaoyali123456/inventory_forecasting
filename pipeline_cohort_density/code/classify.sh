set -exu

DATE=$1
CODE=$2
CODE="s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/pipeline_cohort_density/code/"

aws s3 sync $CODE ~/CODE --quiet
cd ~/CODE

python3 -m pip install --user -r requirements.txt
python3 classify.py $DATE
