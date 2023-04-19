import sys
from datetime import datetime, timedelta

import pandas as pd
from common import *
from prophet import Prophet

DAU_store = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_v3/truth/'
forecast_store = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_v3/forecast/'
AU_table = 'data_warehouse.watched_video_daily_aggregates_ist'

def get_last_cd():
    return str(spark.read.parquet(DAU_store).selectExpr('max(cd) as cd').head().cd)

def truth(end):
    # generate for [begin+1, end]
    last = get_last_cd()
    old = spark.read.parquet(f'{DAU_store}cd={last}')
    new = spark.sql(f'select cd as ds, dw_p_id from {AU_table} where cd > "{last}" and cd <= "{end}"') \
        .where('lower(subscription_status) in ("active", "cancelled", "graceperiod")') \
        .groupby('ds').agg(
            F.countDistinct('dw_p_id').alias('vv'),
            F.countDistinct('dw_p_id').alias('sub_vv')
        )
    old.union(new).repartition(1).write.parquet(new_path)

def predict(df, holidays):
    model = Prophet(holidays=holidays)
    model.add_country_holidays(country_name='IN')
    model.fit(df)
    future = model.make_future_dataframe(periods=365)
    forecast = model.predict(future)
    return model, forecast

def forecast(end):
    df = pd.read_parquet(new_path)
    holidays = pd.read_csv('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/holidays/latest/holidays_v2_4.csv')
    _, f = predict(df.rename(columns={'vv': 'y'}), holidays)
    _, f2 = predict(df.rename(columns={'sub_vv': 'y'}), holidays)
    pd.concat([f.ds.rename('cd'), f.yhat.rename('DAU'), f2.yhat.rename('subs_DAU')], axis=1) \
        .to_parquet(f'{forecast_store}cd={end}/p0.parquet')

if __name__ == '__main__':
    rundate = sys.argv[1]
    end = (datetime.fromisoformat(rundate) - timedelta(1)).date().isoformat()
    new_path = f'{DAU_store}cd={end}/'
    if not s3.isfile(new_path + '_SUCCESS'):
        truth(end)
    forecast(end)
