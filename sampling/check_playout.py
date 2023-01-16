import pandas as pd
import datetime

header='Sr. No.,Start Date,Start Time,Playout ID,Content ID,Language,Tenant,Stream Type,Platform,Creative ID,Break ID,Creative Path,End Date,End Time,Actual Time,Delivered Time,cd'.split(',')
neo = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/playout_v2/'

df=spark.read.csv(neo, header=True).toPandas()
assert list(df.columns) == header

def to_date_safe(dt):
    try:
        return pd.to_datetime(dt)
    except:
        return None

df['start'] = (df.cd.map(str) + ' ' + df['Start Time']).map(to_date_safe)
df['end'] = (df.cd.map(str) + ' ' + df['End Time']).map(to_date_safe)

print(df[df.start.isna()]['Start Time'])
print(df[df.end.isna()]['End Time']) # TODO: to fix: 10/17/2022

x = df[~df.start.isna()].start.map(lambda t: t.time())
print(df[~df.start.isna()][x.apply(lambda t: t < datetime.time(5,30))].cd.drop_duplicates())
# problematic date:
#   2022-11-06
#         hr ad_break_count
#         4      28
#         5     299
#         6     727
#         7     647
#         8     554
#         9     422
#         10    685
#         11    589
#         12    601
#         13    439
#         14    738
#         15    712
#         16    621
#         17     42
#   2021-11-14: no problem
#         18    352
#         19    583
#         20    793
#         21    664
#         22    507
#         23     81
#         0       2

print(x[df[~df.start.isna()].cd == pd.to_datetime('2022-11-06')].apply(lambda x: x.hour).value_counts().sort_index())
print(x[df[~df.start.isna()].cd == pd.to_datetime('2021-11-14')].apply(lambda x: x.hour).value_counts().sort_index())

# check End date
y = df[~df.end.isna()].end.map(lambda t: t.time())
print(df[~df.end.isna()][y.apply(lambda t: t < datetime.time(5, 30))].cd.drop_duplicates())
