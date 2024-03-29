"""
1. update daily gt sub/free DAU from wv_aggr table
2. use Prophet model to predict sub/free DAU
3. combine the gt DAU with predicted DAU
"""
import sys

import pandas as pd
from prophet import Prophet
import pyspark.sql.functions as F

from util import *
from path import *


# generate daily vv/sub_vv between [begin+1, end)
def truth(end, true_vv_path):
    # XXX: get_last_cd is exclusive on `end`, but this is OK given _SUCCESS file check
    last = get_last_cd(DAU_TRUTH_PATH, end)
    old = spark.read.parquet(f'{DAU_TRUTH_PATH}cd={last}').withColumn('free_vv', F.expr('vv - sub_vv'))
    actual_last = old.select('ds').toPandas()['ds'].max()
    if not isinstance(actual_last, str):
        actual_last = str(actual_last.date())
    base = spark.sql(f'select cd as ds, dw_p_id, subscription_status from {DAU_TABLE} where cd > "{actual_last}" and cd < "{end}"')
    new_vv = base.groupby('ds').agg(F.countDistinct('dw_p_id').alias('vv'))
    new_vv.orderBy('ds', ascending=False).show()
    new_sub_vv = base.where('lower(subscription_status) in ("active", "cancelled", "graceperiod")') \
        .groupby('ds').agg(F.countDistinct('dw_p_id').alias('sub_vv'))
    new = new_vv.join(new_sub_vv, on='ds').withColumn('free_vv', F.expr('vv - sub_vv'))
    old.union(new).repartition(1).write.mode('overwrite').parquet(true_vv_path)


# make predictions by Prophet model
def predict(df, holidays):
    model = Prophet(holidays=holidays)
    model.add_country_holidays(country_name='IN')
    model.fit(df)
    future = model.make_future_dataframe(periods=365)
    forecast = model.predict(future)
    return model, forecast


# predict daily vv/sub_vv by Prophet model
def forecast(end, true_vv_path):
    # df = pd.read_parquet(new_path) # pandas read has problem
    df = spark.read.parquet(true_vv_path).toPandas()
    # sub_holidays = pd.read_csv(SUB_HOLIDAYS_FEATURE_PATH)  # TODO: this should be automatically updated.
    # free_holidays = pd.read_csv(FREE_HOLIDAYS_FEATURE_PATH)  # TODO: this should be automatically updated.
    # _, f = predict(df.rename(columns={'vv': 'y'}), free_holidays)
    # _, f2 = predict(df.rename(columns={'sub_vv': 'y'}), sub_holidays)
    # _, f3 = predict(df.rename(columns={'free_vv': 'y'}), free_holidays)
    last_cd = get_last_cd(PROPHET_HOLIDAYS_PATH)
    holidays = pd.read_csv(f"{PROPHET_HOLIDAYS_PATH}/cd={last_cd}/holidays.csv")
    _, f = predict(df.rename(columns={'vv': 'y'}), holidays)
    _, f2 = predict(df.rename(columns={'sub_vv': 'y'}), holidays)
    _, f3 = predict(df.rename(columns={'free_vv': 'y'}), holidays)
    forecast_df = pd.concat([f.ds.astype(str).str[:10], f.yhat.rename('vv'), f2.yhat.rename('sub_vv'), f3.yhat.rename('free_vv')], axis=1)
    # print(forecast_df.ds)
    forecast_df.to_parquet(f'{DAU_FORECAST_PATH}cd={end}/forecast.parquet')


# combine true vv of (, run_date) and predicted vv of [run_date, )
def combine(run_date):
    truth_df = spark.read.parquet(f'{DAU_TRUTH_PATH}cd={run_date}/')
    cols = truth_df.columns
    forecast_df = spark.read.parquet(f'{DAU_FORECAST_PATH}cd={run_date}/')\
        .where(f'ds >= "{run_date}"')\
        .select(cols)
    truth_df.union(forecast_df).repartition(1).write.mode('overwrite').parquet(f'{DAU_COMBINE_PATH}cd={run_date}/')


if __name__ == '__main__':
    # spark.stop()
    spark = hive_spark("dau_update")
    run_date = sys.argv[1]
    true_vv_path = f'{DAU_TRUTH_PATH}cd={run_date}/'
    truth(run_date, true_vv_path)
    forecast(run_date, true_vv_path)
    combine(run_date)
