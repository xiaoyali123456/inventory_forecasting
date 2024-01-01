"""
1. update booking tool request as finished status
2. trigger airflow job to fetch inventory prediction result on booking tool side
"""
import requests
import sys
import os
import pandas as pd

from path import *


def check_s3_path_exist(s3_path: str) -> bool:
    if not s3_path.endswith("/"):
        s3_path += "/"
    return os.system(f"aws s3 ls {s3_path}_SUCCESS") == 0


# trigger airflow job to fetch inventory prediction result on booking tool side
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


def update_request_status_and_trigger_airflow(DATE):
    df = pd.read_parquet(f'{FINAL_ALL_PREDICTION_PATH}cd={DATE}/')
    for r_id in set(df.inventoryId):
        r = requests.patch(
            BOOKING_TOOL_URL + f'api/v1/inventory/{r_id}/ad-placement/MIDROLL/forecast-request',
            json={
                "requestStatus": "SUCCESS",
            }
        )
        print('updated status:', r.status_code)
    trigger_airflow(DATE)


if __name__ == '__main__':
    DATE = sys.argv[1]
    if check_s3_path_exist(f'{PREDICTION_MATCH_TABLE_PATH}/cd={DATE}/'):
        update_request_status_and_trigger_airflow(DATE)

