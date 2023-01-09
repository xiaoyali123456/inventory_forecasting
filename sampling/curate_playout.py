header='Sr. No.,Start Date,Start Time,Playout ID,Content ID,Language,Tenant,Stream Type,Platform,Creative ID,Break ID,Creative Path,End Date,End Time,Actual Time,Delivered Time'
from glob import glob
import os
import json

old = 's3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test/'
neo = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/playout/'

def clone():
    tournaments = ['wc2022'] # ['wc2021', 'wc2022']
    for tour in tournaments:
        dates_json = tour + '.json'
        if not os.path.exists(dates_json):
            os.system('aws s3 sync s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/valid_dates/ .')
        with open(tour+'.json') as f:
            dates = json.load(f)
        for dt in dates:
            os.system(f'aws s3 sync {old}{dt} {neo}cd={dt}')


def curate(inp, out=None):
    beforeHead = True
    cnt = 0
    with open(inp, encoding='latin1') as fin:
        try:
            for i, ln in enumerate(fin):
                if beforeHead:
                    if ',,,,,' in ln:
                        cnt+=1
                        continue
                    elif 'Playout ID' in ln:
                        beforeHead = False
                    else:
                        print('err1', i)
                        break
                else:
                    break
        except:
            print('err2', i)
    print(inp, cnt)
    if out:
        os.makedirs(os.path.dirname(out))
        with open(inp, encoding='latin1') as fin:
            with open(out, 'w') as fout:
                for i, ln in enumerate(fin):
                    if i >= cnt:
                        fout.write(ln)

if __name__ == "__main__":
    os.system(f'aws s3 sync {neo} playout')
    for f in glob('playout/*/*.csv'):
        curate(f, f.replace('playout', 'playout_v2', 1))
