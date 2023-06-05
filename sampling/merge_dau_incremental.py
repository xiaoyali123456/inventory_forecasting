#merge dau incremental
import pandas as pd
from prophet import Prophet
HOLIDAYS_FEATURE_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/holidays/latest/holidays_v2_4.csv'

gt = pd.read_parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_v3/truth_back_up_before_inc/cd=2023-05-29/')
inc = pd.read_parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/dau_incremental')
inc.date = inc.date.map(str)
inc.dau_incremental = inc.dau_incremental.map(float)

up = gt.merge(inc, how='left', left_on='ds', right_on='date')
up.dau_incremental.fillna(0.0, inplace=True)
up.vv += up.dau_incremental

up.drop(columns=['date', 'dau_incremental']).to_parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_v3/truth/cd=2023-05-29/part-00000-3d21ceae-45e9-4e9c-831d-0b17c1c8e948-c000.snappy.parquet', index=False)
df = pd.read_parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_v3/truth/cd=2023-05-29/')

def predict(df, holidays):
    model = Prophet(holidays=holidays)
    model.add_country_holidays(country_name='IN')
    model.fit(df)
    future = model.make_future_dataframe(periods=365)
    forecast = model.predict(future)
    return model, forecast

holidays = pd.read_csv(HOLIDAYS_FEATURE_PATH)
_, f = predict(df.rename(columns={'vv': 'y'}), holidays)
_, f2 = predict(df.rename(columns={'sub_vv': 'y'}), holidays)
df2 = pd.concat([f.ds.rename('cd'), f.yhat.rename('DAU'), f2.yhat.rename('subs_DAU')], axis=1)
