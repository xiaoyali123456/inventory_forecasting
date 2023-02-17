from functools import reduce
import pandas as pd
import pyspark.sql.functions as F

def load_playout():
    path = [
        's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/playout_v3/wc2022/',
        's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/playout_v3/wc2021/',
    ]
    df = reduce(lambda x,y: x.union(y), [spark.read.parquet(i) for i in path])
    df = df.selectExpr(
        'trim(content_id) as content_id',
        'trim(playout_id) as playout_id',
        'trim(platform) as platform',
        'trim(content_language) as language',
        'start_time',
    )
    return df.groupby('content_id', 'playout_id').agg(
        F.min('start_time').alias('start_time'),
        F.min('platform').alias('platform'),
        F.min('language').alias('language'))

def load_inventory():
    path = [
        # XXX: inventory contain "Follow-on" match, but playout_v3 don't.
        's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/inventory_wt/cohort_agg_quarter/tournament=wc2022',
        's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/inventory_wt/cohort_agg_quarter/tournament=wc2021',
    ]
    df = reduce(lambda x,y: x.union(y), [spark.read.parquet(i) for i in path])
    return df

def parse(ssai, prefix='M_'):
    if isinstance(ssai, str):
        for x in ssai.split(':'):
            if x.startswith(prefix):
                return x
    return ''

pl = load_playout()
iv = load_inventory()
df = iv.join(pl, on=['content_id', 'playout_id']).toPandas()
df['ad_time_ratio'] = df.ad_time / df.groupby(['content_id', 'playout_id'])['ad_time'].transform('sum')
# df2 = df.groupby(['cohort', 'start_time'])['ad_time_ratio'].sum().reset_index()
# df.to_parquet('ema_analysis_df.parquet')

df['tag'] = df.cohort.apply(parse)
df2=df.groupby(['start_time', 'tag'])['ad_time_ratio'].sum().reset_index()
# clear
df3=df2.pivot('start_time', 'tag', 'ad_time_ratio')
lam,mu = 0.9,0.1
for x in df3.columns:
    lst = [0]
    for y in df3[x][1:]:
        z, w = 0, 0
        if lst[-1] == lst[-1]:
            z += lam * lst[-1]
            w += lam
        if y == y:
            z += mu * y
            w += mu
        if w == 0:
            z = 0
        else:
            z /= w
        lst.append(z)
    df3[x+'_pr'] = lst
    df3[x+'_err'] = df3[x+'_pr']-df3[x]
    df3[x+'_rerr'] = df3[x+'_err']/df3[x]

df3.to_csv('df2.csv')

