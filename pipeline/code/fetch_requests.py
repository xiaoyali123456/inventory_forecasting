import pandas as pd
import sys
import json
from subprocess import check_output

SERVER_URL_ROOT = 'http://localhost:4321/'

def main(cd):
    req_lst = []
    i, tot = 1, 1
    while i <= tot:
        df = pd.read_json(f'{SERVER_URL_ROOT}inventory/forecast-request?'
                        'status=INIT'
                        f'&page-number={i}'
                        '&page-size=1'
                        )
        req_lst += df.results.tolist()
        tot = df.total_pages[0]
        i += 1
    print(req_lst)
    tmp = '/tmp/tmp.json'
    out = f's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/inventory_requests/cd={cd}/requests.json'
    with open(tmp, 'w') as f:
        json.dump(req_lst, f)
    check_output(['aws', 's3', 'cp', tmp, out])


if __name__ == '__main__':
    cd = sys.argv[1]
    main(cd)
