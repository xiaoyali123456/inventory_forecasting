from prophet import Prophet
import pandas as pd

holidays = pd.read_csv('holidays_v2_3.csv')
df = spark.read.parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_full_v2/all/cd=2023-04-11/').toPandas()

def predict(df):
    m = Prophet(holidays=holidays)
    m.add_country_holidays(country_name='IN')
    m.fit(df)
    future = m.make_future_dataframe(periods=365)
    forecast = m.predict(future)
    return m, forecast

m, f = predict(df.rename(columns={'vv': 'y'}))
m2, f2 = predict(df.rename(columns={'sub_vv': 'y'}))

m.plot(f);
m2.plot(f2);

# line-line plot
f.merge(df.vv, left_on=f.ds.map(lambda x:str(x.date())), right_on=df.ds, how='left') \
    .plot(x='ds', y=['vv', 'yhat'], figsize=(25, 10), alpha=0.75, grid=True);
f2.merge(df.sub_vv, left_on=f.ds.map(lambda x:str(x.date())), right_on=df.ds, how='left') \
    .plot(x='ds', y=['sub_vv', 'yhat'], figsize=(25, 10), alpha=0.75, grid=True);

pd.concat([f.ds.rename('cd'), f.yhat.rename('DAU'), f2.yhat.rename('subs_DAU')], axis=1).to_parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_predict/v2/cd=2023-04-11/p0.parquet')

# compare the difference with last prediction
t = spark.read.parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_predict/DAU_predict.parquet').toPandas()
t2 = spark.read.parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_predict/v2/cd=2023-04-11/p0.parquet').toPandas()

x = t[(t.cd >= '2023-10-10') & (t.cd <= '2023-11-26')]
x2 = t2[(t2.cd >= '2023-10-10') & (t2.cd <= '2023-11-26')]

y = x.merge(x2, on='cd')
display((y['DAU_y']/y['DAU_x']).describe().reset_index())
display((y['subs_DAU_y']/y['subs_DAU_x']).describe().reset_index())

sub_x = y['subs_DAU_x'] / y['DAU_x']
sub_y = y['subs_DAU_y'] / y['DAU_y']
display((sub_y/sub_x).describe().reset_index())


# try leaking the label to prophet
last = pd.DataFrame([
    ['2019-06-16', 'super', 0, 0],
    ['2019-06-30', 'super', 0, 0],
    ['2019-07-02', 'super', 0, 0],
    ['2019-07-09', 'super', 0, 0],
    ['2019-07-10', 'super', 0, 0],
    ['2019-07-10', 'super', 0, 0],
    ['2023-10-28', 'super', 0, 0],
    ['2023-11-03', 'super', 0, 0],
    ['2023-11-11', 'super', 0, 0],
    ['2023-11-13', 'super', 0, 0],
], columns=holidays.columns)
h2 = pd.concat([holidays, last], ignore_index=True)

def predict(df, feature):
    m = Prophet(holidays=feature)
    m.add_country_holidays(country_name='IN')
    m.fit(df)
    future = m.make_future_dataframe(periods=365)
    forecast = m.predict(future)
    return m, forecast

m, f = predict(df.rename(columns={'vv': 'y'}), h2)
m.plot(f, figsize=(16, 10));

# save
pd.concat([f.ds.rename('cd'), f.yhat.rename('DAU'), f2.yhat.rename('subs_DAU')], axis=1).to_parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_predict/v3/cd=2023-04-11/p0.parquet')

# calculate the ratio of cwc 2023
t = spark.read.parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_predict/DAU_predict.parquet').toPandas()
t2 = spark.read.parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_predict/v3/cd=2023-04-11/p0.parquet').toPandas()
t.cd = t.cd.map(lambda x: x.date().isoformat() if not isinstance(x, str) else x)
t2.cd = t2.cd.map(lambda x: x.date().isoformat() if not isinstance(x, str) else x)

x = t[(t.cd >= '2023-10-10') & (t.cd <= '2023-11-26')]
x2 = t2[(t2.cd >= '2023-10-10') & (t2.cd <= '2023-11-26')]

y = x.merge(x2, on='cd')
display((y['DAU_y']/y['DAU_x']).describe().reset_index())
display((y['subs_DAU_y']/y['subs_DAU_x']).describe().reset_index())

sub_x = y['subs_DAU_x'] / y['DAU_x']
sub_y = y['subs_DAU_y'] / y['DAU_y']
display((sub_y/sub_x).describe().reset_index())


# alpha rendering
tmp = f_if.merge(df.vv, left_on=f_if.ds.map(lambda x:str(x.date())), right_on=df.ds, how='left')
tmp[tmp.ds.map(lambda x:str(x.date())) > '2019-02-10'].plot(x='ds', y=['vv', 'yhat'], figsize=(25, 10), alpha=0.75, grid=True);

tmp = f2_if.merge(df.sub_vv, left_on=f2_if.ds.map(lambda x:str(x.date())), right_on=df.ds, how='left')
tmp[tmp.ds.map(lambda x:str(x.date())) > '2019-02-10'].plot(x='ds', y=['sub_vv', 'yhat'], figsize=(25, 10), alpha=0.75, grid=True);


