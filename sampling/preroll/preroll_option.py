import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_preroll/inventory/'

# cd_list = ['2023-06-10', '2023-06-11']
cd_list = ['2023-06-07']
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
    'location_cluster': [0.95, 0.97, 0.99],
    'pincode': [0.78, 0.78, 0.95],
    'platform': [1.0, 1.0, 1.0],
    'state': [0.95, 0.97, 0.99],
    'subscription_type': [0.95, 0.97, 0.99],
}

res = {}
for k, v in list(threshold.items()):
    df2 = df.groupby(k).agg(F.sum('inventory').alias('inventory')).toPandas()
    df3 = df2.sort_values('inventory', ascending=False).reset_index(drop=True)
    for t in v:
        df4 = df3[df3.inventory.cumsum().shift(fill_value=0) <= df3.inventory.sum()*t]
        res[k, t] = df4


total = df[list(threshold)].distinct().toPandas()
for i in range(3):
    total['mask'] = True
    for col in threshold:
        total['mask'] = total['mask'] & total[col].isin(res[col, threshold[col][i]][col])
    print(i)
    print('col', 'threshold', 'num')
    for col in threshold:
        print(col, threshold[col][i], len(res[col, threshold[col][i]]))
    print('cohort_num', sum(total['mask']))
