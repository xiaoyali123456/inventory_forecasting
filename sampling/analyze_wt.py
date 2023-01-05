import numpy as np

N=10

# wt_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/sampling/cohort_agg/'
wt_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/dw_d_id/cohort_agg/'
wt = spark.read.parquet(wt_path).toPandas()
wt2 = wt.groupby(['cd', 'content_id', 'cohort']).sum().reset_index()
wt_top_ssai = wt2.groupby('cohort').sum()['ad_time'].nlargest(N).index

cc_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/sampling/inventory_concurrency/'
cc = spark.read.parquet(cc_path).toPandas()
cc2 = cc.rename(columns={'ssai_tag':'cohort'}) \
    .groupby(['cd', 'content_id', 'cohort']).sum().reset_index()

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
    return np.dot(a,b)/np.linalg.norm(a)/np.linalg.norm(b)

wt_cc = wt5.join(cc5, how='outer', rsuffix='_cc').fillna(0.0)
print(wt_cc.groupby('cd').apply(corr))
