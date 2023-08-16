path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_preroll/inventory/'

# cd_list = ['2023-06-10', '2023-06-11']
cd_list = ['2023-06-18']
df = None
for cd in cd_list:
    tmp = spark.read.parquet(path+'cd='+cd)
    if df is None:
        df = tmp
    else:
        df = df.union(tmp)

threshold = {
    'city': [0.95, 0.97, 0.99],
    'data_sim': [0.95, 0.97, 0.99],
    'device_brand': [0.95, 0.97, 0.99],
    'device_model': [0.95, 0.97, 0.99],
    'gender': [0.95, 0.97, 0.99],
    'location_cluster': [],
    'pincode': [0.95, 0.97, 0.99],
    'platform': [1.0],
    'state': [0.95, 0.97, 0.99],
    'subscription_type': [0.95, 0.97, 0.99],
}

for k, v in list(threshold.items()):
    print(k)
    for t in v:
        df2 = df.groupby(k).agg(F.sum('reach').alias('reach')).toPandas()
        df3 = df2.sort_values('reach', ascending=False).reset_index(drop=True).cumsum()
        df4 = df3[df3.reach < df3.reach.iloc[-1]*t]

