import sys

import pandas as pd
from prophet import Prophet
from common import *


# XXX: due to historical reason, `AU` in this project means `VV`
# TODO: we should consider whether we should refactor this
# generate for [begin+1, end]
def truth(end, new_path):
    # XXX: get_last_cd is exclusive on `end`, but this is OK given _SUCCESS file check
    last = get_last_cd(DAU_TRUTH_PATH, end)
    old = spark.read.parquet(f'{DAU_TRUTH_PATH}cd={last}')
    actual_last = old.select('ds').toPandas()['ds'].max()
    if not isinstance(actual_last, str):
        actual_last = str(actual_last.date())
    base = spark.sql(f'select cd as ds, dw_p_id, subscription_status from {DAU_TABLE} where cd > "{actual_last}" and cd < "{end}"')
    new_vv = base.groupby('ds').agg(F.countDistinct('dw_p_id').alias('vv'))
    new_sub_vv = base.where('lower(subscription_status) in ("active", "cancelled", "graceperiod")') \
        .groupby('ds').agg(F.countDistinct('dw_p_id').alias('sub_vv'))
    new = new_vv.join(new_sub_vv, on='ds')
    old.union(new).repartition(1).write.mode('overwrite').parquet(new_path)


def predict(df, holidays):
    model = Prophet(holidays=holidays)
    model.add_country_holidays(country_name='IN')
    model.fit(df)
    future = model.make_future_dataframe(periods=365)
    forecast = model.predict(future)
    return model, forecast


def forecast(end, new_path):
    # df = pd.read_parquet(new_path) # pandas read has problem
    df = spark.read.parquet(new_path).toPandas()
    holidays = pd.read_csv(HOLIDAYS_FEATURE_PATH) # TODO: this should be automatically updated.
    _, f = predict(df.rename(columns={'vv': 'y'}), holidays)
    _, f2 = predict(df.rename(columns={'sub_vv': 'y'}), holidays)
    pd.concat([f.ds, f.yhat.rename('vv'), f2.yhat.rename('sub_vv')], axis=1) \
        .to_parquet(f'{DAU_FORECAST_PATH}cd={end}/forecast.parquet')


def combine(rundate):
    truth_df = spark.read.parquet(f'{DAU_TRUTH_PATH}cd={rundate}/')
    cols = truth_df.columns
    forecast_df = spark\
        .read\
        .parquet(f'{DAU_FORECAST_PATH}cd={rundate}/')\
        .where(f'ds >= "{rundate}"')\
        .select(cols)
    truth_df.union(forecast_df).repartition(1).write.mode('overwrite').parquet(f'{DAU_COMBINE_PATH}cd={rundate}/')


if __name__ == '__main__':
    rundate = sys.argv[1]
    new_path = f'{DAU_TRUTH_PATH}cd={rundate}/'
    if not s3.isfile(new_path + '_SUCCESS'):
        truth(rundate, new_path)
    forecast(rundate, new_path)
    combine(rundate)
