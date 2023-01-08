# sync playout data and clean up
# !aws s3 sync s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/valid_dates/ .
import os
import json

old = 's3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test/'
neo = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/playout/'

tournament = 'wc2021'
with open(tournament+'.json') as f:
    dates = json.load(f)
for dt in dates:
    os.system(f'aws s3 sync {old}{dt} {neo}cd={dt}')

## bash script to process playout csv
# for f in /tmp/playout/*/*.csv;do
#     python3 a.py "$f" "/home/hadoop/minliang/playout/$f" >> a.log
# done
