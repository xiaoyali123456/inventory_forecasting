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
final = wt3.join(cc3, how='outer', rsuffix='_cc')
final.to_csv(f'top{N}.csv')
