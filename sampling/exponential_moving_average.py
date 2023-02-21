from functools import reduce
import pandas as pd
import numpy as np
from tqdm import tqdm
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

def moving_avg(df, lam=0.8, prefix='M_'):
    df['tag'] = df.cohort.apply(lambda x: parse(x, prefix)) # customize this line
    time_col = 'cd' # 'start_time'
    df2=df.groupby([time_col, 'tag'])['ad_time'].sum().reset_index()
    df2['ad_time_ratio'] = df2.ad_time/df2.groupby([time_col])['ad_time'].transform('sum')
    df3=df2.pivot(time_col, 'tag', 'ad_time_ratio')
    mu = 1 - lam
    tags = set(df2.tag)
    for x in tags:
        lst = [0]
        for y in df3[x][:-1]:
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
    cols = []
    for x in tags:
        cols += [x, x+'_pr']
    for x in tags:
        cols.append(x+'_err')
    for x in tags:
        cols.append(x+'_rerr')
    df3 = df3[cols]
    last_n = 30
    df4 = df3[[x + '_err' for x in tags]].fillna(0)[last_n:]
    df5 = df2.pivot(time_col, 'tag', 'ad_time')[tags].fillna(0) # inventory
    df6 = df5.to_numpy()[last_n:]
    df7 = df6 * df4.abs().to_numpy() # abs error
    df8 = df6 * df4.to_numpy()
    print('inventory', df6.sum(),
          'abs_err', df7.sum()/df6.sum(),
          'sum_err', df8.sum()/df6.sum())
    return df3

def moving_avg2(df, lam=0.8, prefix='M_'):
    time_col = 'cd' # 'start_time'
    df2=df.groupby([time_col, 'cohort'])['ad_time'].sum().reset_index()
    df3=df2.pivot(time_col, 'cohort', 'ad_time').fillna(0)
    mu = 1 - lam
    # x is the sum
    fun = np.frompyfunc(lambda x,y: lam * x + mu * y, 2, 1)
    cohorts = set(df2.cohort)
    for x in tqdm(cohorts):
        df3[x+'_pr'] = fun.accumulate(df3[x], dtype=object)
    last_n = 30
    dc = {}
    for x in cohorts:
        t = parse(x)
        if t not in dc:
            dc[t] = []
        dc[t].append(x)
    for x in dc:
        pd.DataFrame([df3[dc[x]].sum(axis=1),
        df3[[s+'_pr' for s in dc[x]]].sum(axis=1)])
    return df3

if __name__ == '__main__':
    pl = load_playout()
    iv = load_inventory()
    df = iv.join(pl, on=['content_id', 'playout_id']).toPandas()
    df.to_parquet("ema.parquet")
    df3 = moving_avg(df, prefix='M_')
    df3.to_csv('df2.csv')

