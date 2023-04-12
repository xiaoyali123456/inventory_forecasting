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

pd.concat([f.ds.rename('cd'), f.yhat.rename('DAU'), f2.yhat.rename('subs_DAU')], axis=1).to_parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_predict/cd=2023-04-11/p0.parquet')

# compare the difference with last prediction
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

