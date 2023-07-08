import requests
import sys
import pandas as pd

from common import BOOKING_TOOL_URL, FINAL_ALL_PREDICTION_PATH

def trigger_airflow(cd):
    r = requests.post(
        'https://airflow-prod.data.k8s.hotstar-labs.com/api/experimental/dags/adtech_prod_midroll_inventory_forecast/dag_runs',
        json={
            'conf': {
                'data': {
                    'ad_placement': 'MIDROLL',
                    's3_path': f's3a://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/final/all_tournament_partition/cd={cd}/'
                }
            }
        }
    )
    return r.status_code

if __name__ == '__main__':
    DATE = sys.argv[1]
    df = pd.read_parquet(f'{FINAL_ALL_PREDICTION_PATH}cd={DATE}/')
    for r_id in set(df.inventoryId):
        r = requests.patch(
            BOOKING_TOOL_URL + f'inventory/{r_id}/ad-placement/MIDROLL/forecast-request',
            json = {
                "request_status": "SUCCESS",
                "version": 1,
            }
        )
        print('updated status:', r.status_code)
    # trigger_airflow(DATE) # TODO: open this
