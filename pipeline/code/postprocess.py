import requests
import sys
import pandas as pd
from common import BOOKING_TOOL_URL, FINAL_ALL_PREDICTION_PATH

if __name__ == '__main__':
    DATE = sys.argv[1]
    df = pd.read_parquet(f'{FINAL_ALL_PREDICTION_PATH}cd={DATE}/')
    for r_id in set(df.request_id):
        r = requests.patch(
            BOOKING_TOOL_URL + f'inventory/{r_id}/ad-placement/MIDROLL/forecast-request',
            json = {
                "request_status": "SUCCESS",
                "version": 1,
            }
        )
        print('updated status:', r.status_code)
