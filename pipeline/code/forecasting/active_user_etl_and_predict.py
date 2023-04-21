import sys
from datetime import datetime, timedelta

import pandas as pd
from prophet import Prophet
from common import *

def get_last_cd(end):
    df = spark.read.parquet(DAU_TRUTH_PATH).where('cd < "{end}"')
    return str(df.selectExpr('max(cd) as cd').head().cd)

# generate for [begin+1, end]
def truth(end):
    last = get_last_cd(end)
    old = spark.read.parquet(f'{DAU_TRUTH_PATH}cd={last}')
    new = spark.sql(f'select cd as ds, dw_p_id from {DAU_TABLE} where cd > "{last}" and cd <= "{end}"') \
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
    holidays = pd.read_csv(HOLIDAYS_FEATURE_PATH)
    _, f = predict(df.rename(columns={'vv': 'y'}), holidays)
    _, f2 = predict(df.rename(columns={'sub_vv': 'y'}), holidays)
    pd.concat([f.ds.rename('cd'), f.yhat.rename('DAU'), f2.yhat.rename('subs_DAU')], axis=1) \
        .to_parquet(f'{DAU_FORECAST_PATH}cd={end}/forecast.parquet')

if __name__ == '__main__':
    rundate = sys.argv[1]
    end = (datetime.fromisoformat(rundate) - timedelta(1)).date().isoformat()
    new_path = f'{DAU_TRUTH_PATH}cd={end}/'
    if not s3.isfile(new_path + '_SUCCESS'):
        truth(end)
    forecast(end)
