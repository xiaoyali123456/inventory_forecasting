import json
import os
from datetime import datetime

output_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/watched_video/'
playout_log_path = 's3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test/'
wv_path = 's3://hotstar-dp-datalake-processed-us-east-1-prod/events/watched_video/'

def main():
    tournament='wc2022'
    if os.path.exists(f'{tournament}.json'):
        with open('dates.json') as fp:
            dates = json.load(fp)
    else:
        dates = list(valid_dates(tournament))
        with open(f'{tournament}.json', 'w') as fp:
            json.dump(dates, fp)
    for dt in dates:
        print(dt)
        for i in range(24):
            print(datetime.now(), 'hr:', i)
            path=f'{wv_path}cd={dt}/hr={i:02}'
            path2=f'{output_path}cd={dt}/hr={i:02}'
            if os.system(f'aws s3 ls {path2}/_SUCCESS') != 0:
                os.system(f'aws s3 sync {path} {path2}')

if __name__ == '__main__':
    main()
