import numpy as np

wt_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/inventory_wt/cohort_agg/'
wt = spark.read.parquet(wt_path).toPandas()
cc_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/sampling/inventory_concurrency/'
cc = spark.read.parquet(cc_path).toPandas().rename(columns={'ssai_tag':'cohort'})

def calc_ratio(df, col):
    keys = ['cd', 'content_id', 'cohort']
    df2 = df.groupby(keys).sum().reset_index()
    df2[col + '_ratio'] = df2[col] / df2.groupby(keys[:2])[col].transform('sum')
    return df2

cc2 = calc_ratio(cc, 'ad_time')
wt2 = calc_ratio(wt, 'ad_time')

def save_topn(n=20):
    wt_top_ssai = wt2.groupby('cohort').sum()['ad_time'].nlargest(n).index
    wt3 = wt2[wt2.cohort.isin(wt_top_ssai)].groupby(['cd', 'cohort']).sum()
    cc3 = cc2[cc2.cohort.isin(wt_top_ssai)].groupby(['cd', 'cohort']).sum()
    topn = wt3.join(cc3, how='outer', rsuffix='_cc')
    topn.to_csv(f'top{n}.csv')

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

def corr(x):
    a=x['ad_time_ratio']
    b=x['ad_time_ratio_cc']
    # a=(a-a.mean())/a.std() # Pearson correlation, doesn't make much difference
    # b=(b-b.mean())/a.std()
    return np.dot(a,b)/np.linalg.norm(a)/np.linalg.norm(b)

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

def metric(wt, cc):
    wt_cc = pd.merge(wt, cc, on=['cd', 'content_id', 'cohort'], how='outer', suffixes=['','_cc']).fillna(0.0)
    print(wt_cc.groupby('cd').apply(corr))
    print(wt_cc.groupby('cd').apply(rmse))
    print(wt_cc.groupby('cd').apply(rate_diff))

if __name__ == "__main__":
    metric(wt2, cc2)
    # parse_ssai(wt2).to_csv('wc2022_city_quarter.csv')
