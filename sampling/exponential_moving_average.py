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
        'creative_path',
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
    df2 = df.groupby([time_col, 'tag'])['ad_time'].sum().reset_index()
    df2['ad_time_ratio'] = df2.ad_time/df2.groupby(time_col)['ad_time'].transform('sum')
    tags = list(set(df2.tag))
    tags_pr = [x+'_pr' for x in tags]
    df3 = df2.pivot(time_col, 'tag', 'ad_time_ratio')[tags]
    mu = 1 - lam
    fun = np.frompyfunc(lambda x,y: lam * x + mu * y, 2, 1)
    for x in tags:
        df3[x+'_pr'] = fun.accumulate(df3[x], dtype=object)
    df4 = df3[tags_pr].fillna(0).to_numpy()[:-1] - df3[tags].fillna(0).to_numpy()[1:]
    df5 = df2.pivot(time_col, 'tag', 'ad_time')[tags].fillna(0).to_numpy()[1:]
    df6 = df5 * np.abs(df4) # abs error
    n_last = 29
    print(df5[n_last:].sum(), df6[n_last:].sum()/df5[n_last:].sum())
    return df3

def moving_avg2(df, lam=0.9, prefix='M_'):
    time_col = 'cd' # 'start_time'
    df['cohort_s'] = df.cohort.fillna('') # XXX: critical for groupby None
    df2=df.groupby([time_col, 'cohort_s'])['ad_time'].sum().reset_index()
    df2['ad_time_ratio'] = df2.ad_time/df2.groupby(time_col).transform('sum')
    cohorts = list(set(df2.cohort_s))
    df3=df2.pivot(time_col, 'cohort_s', 'ad_time_ratio').fillna(0)[cohorts]
    mu = 1 - lam
    # x is the sum
    fun = np.frompyfunc(lambda x,y: lam * x + mu * y, 2, 1)
    for x in tqdm(cohorts):
        df3[x+'_pr'] = fun.accumulate(df3[x], dtype=object)
    df3 = df3[cohorts + [x+'_pr' for x in cohorts]]
    df3.columns = pd.MultiIndex.from_tuples([('gt', parse(c), c) for c in cohorts] + 
                                            [('pr', parse(c), c + '_pr') for c in cohorts])
    gt = df3['gt'].sum(level=0, axis=1)
    pr = df3['pr'].sum(level=0, axis=1)
    df4 = pr.to_numpy()[:-1] - gt.to_numpy()[1:]
    df2['tag'] = df2.cohort_s.apply(lambda x: parse(x, prefix))
    df5 = df2.groupby([time_col, 'tag'])['ad_time'].sum().reset_index().pivot(time_col, 'tag', 'ad_time').fillna(0).to_numpy()[1:]
    df6 = df5 * np.abs(df4)
    n_last = 29
    print('inventory', df5[n_last:].sum(),
          'err', df6[n_last:].sum()/df5[n_last:].sum(),
          's_err', (df5 * df4)[n_last:].sum())
    return pd.concat([gt, pr], axis=1)

if __name__ == '__main__':
    pl = load_playout()
    iv = load_inventory()
    df = iv.join(pl, on=['content_id', 'playout_id']).toPandas()
    df3 = moving_avg2(df)
    df3.to_csv('df2.csv')
