import pandas as pd

header='Sr. No.,Start Date,Start Time,Playout ID,Content ID,Language,Tenant,Stream Type,Platform,Creative ID,Break ID,Creative Path,End Date,End Time,Actual Time,Delivered Time,cd'.split(',')
neo = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/playout_v2/'

df=spark.read.csv(neo, header=True).toPandas()
assert list(df.columns) == header

pd.to_datetime(df[df.cd>pd.to_datetime('2022-01-01')]['Start Date'])
pd.to_datetime(df[df.cd>pd.to_datetime('2022-01-01')]['End Date'])
pd.to_datetime(df[df.cd<pd.to_datetime('2022-01-01')]['Start Date']) # exception
pd.to_datetime(df[df.cd<pd.to_datetime('2022-01-01')]['End Date'])
