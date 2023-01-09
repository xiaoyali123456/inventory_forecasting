import numpy as np

N=20
wt_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/inventory_wt/cohort_agg/'
wt = spark.read.parquet(wt_path).toPandas()
wt2 = wt.groupby(['cd', 'content_id', 'cohort']).sum().reset_index()
wt_top_ssai = wt2.groupby('cohort').sum()['ad_time'].nlargest(N).index

cc_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/sampling/inventory_concurrency/'
cc = spark.read.parquet(cc_path).toPandas()
cc2 = cc.rename(columns={'ssai_tag':'cohort'}) \
    .groupby(['cd', 'content_id', 'cohort']).sum().reset_index()

## END of LOAD DATA
def select(ssai):
    head = 'SSAI::'
    n = len(head)
    res = [x for x in ssai[n:].split(':') if x.startswith('M_')]
    return res[0] if len(res) else ''

def parse_ssai(df):
    df['tag'] = df.cohort.apply(select)
    df2=df.groupby(['cd', 'content_id', 'tag']).sum().reset_index()
    df2['ad_time_ratio'] = df2['ad_time']/df2.groupby(['cd', 'content_id'])['ad_time'].transform('sum')
    # df2['reach_ratio'] = df2['reach']/df2.groupby(['cd', 'content_id'])['reach'].transform('sum')
    return df2.pivot(index=['cd', 'content_id'], columns='tag', values='ad_time_ratio').fillna(0)

parse_ssai(wt2).to_csv('wc2021_city.csv')

# 
wt3 = wt2[wt2.cohort.isin(wt_top_ssai)].groupby(['cd', 'cohort']).sum()
cc3 = cc2[cc2.cohort.isin(wt_top_ssai)].groupby(['cd', 'cohort']).sum()
topn = wt3.join(cc3, how='outer', rsuffix='_cc')
topn.to_csv(f'top{N}.csv')

wt4 = wt2.groupby(['cd', 'cohort']).sum()
cc4 = cc2.groupby(['cd', 'cohort']).sum()

wt4s = wt4.groupby('cd').sum()
cc4s = cc4.groupby('cd').sum()

wt5 = wt4.join(wt4s, rsuffix='_sum')
cc5 = cc4.join(cc4s, rsuffix='_sum')

wt5['ad_time_ratio'] = wt5.ad_time/wt5.ad_time_sum
wt5['reach_ratio'] = wt5.reach/wt5.reach_sum
cc5['ad_time_ratio'] = cc5.ad_time/cc5.ad_time_sum

def corr(x):
    a=x['ad_time_ratio']
    b=x['ad_time_ratio_cc']
    # a=(a-a.mean())/a.std() # Pearson correlation, doesn't make much difference
    # b=(b-b.mean())/a.std()
    return np.dot(a,b)/np.linalg.norm(a)/np.linalg.norm(b)

wt_cc = wt5.join(cc5, how='outer', rsuffix='_cc').fillna(0.0)
print(wt_cc.groupby('cd').apply(corr))

def rmse(x):
    a=x['ad_time_ratio']
    b=x['ad_time_ratio_cc']
    return np.sqrt(np.mean((a-b)**2))

def rate_diff(x):
    a=x['ad_time_ratio']
    b=x['ad_time_ratio_cc']
    c = np.abs(a-b)
    d = b.apply(lambda x: 1 if x <= 0 else x)
    return np.mean(c/d)

wt_cc = wt5.join(cc5, how='outer', rsuffix='_cc').fillna(0.0)
print(wt_cc.groupby('cd').apply(rmse))
print(wt_cc.groupby('cd').apply(rate_diff))
