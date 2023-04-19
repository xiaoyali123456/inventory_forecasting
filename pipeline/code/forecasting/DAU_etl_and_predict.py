import sys
from datetime import datetime, timedelta

from prophet import Prophet
import pandas as pd

from common import *

store = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/truth/'
AU_table = 'data_warehouse.watched_video_daily_aggregates_ist'

def get_last_cd():
    return str(spark.read.parquet(store).selectExpr('max(cd) as cd').head().cd)

def dau(end):
    # generate for [begin+1, end]
    last = get_last_cd()
    old = spark.read.parquet(f'{store}cd={last}')
    new = spark.sql(f'select cd as ds, dw_p_id from {AU_table} where cd > "{last}" and cd <= "{end}"') \
        .where('lower(subscription_status) in ("active", "cancelled", "graceperiod")') \
        .groupby('ds').agg({
            'vv': F.distinctCount('dw_p_id'),
            'sub_vv': F.distinctCount('dw_p_id'),
        })
    old.union(new).repartition(1).write.parquet(f'{store}cd={end}')

def forecast(end):
    df = spark.read.parquet(new_path)
    holidays = pd.read_csv('holidays_v2_3.csv')

if __name__ == '__main__':
    rundate = sys.argv[1]
    end = (datetime.fromisoformat(rundate) - timedelta(1)).date().isoformat()
    new_path = f'{store}cd={end}'
    if not s3.isfile(new_path):
        dau(end)
    forecast(end)
