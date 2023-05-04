import pandas as pd

out_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/custom_cohort_inventory_v1/'
df = spark.read.parquet(out_path).toPandas().fillna('')
df2 = df[df.country == 'in'].groupby(['is_cricket', 'cd', 'segments'])['watch_time'].sum().reset_index()
df2['wt%'] = df2['watch_time'] / df2.groupby(['is_cricket', 'cd'])['watch_time'].transform(sum)
print(df2.to_csv(index=False))

# Update to v3
out_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/custom_cohort_inventory_v2/'
df = spark.read.parquet(out_path).toPandas().fillna('')
df2 = df[df.country == 'in'].groupby(['is_cricket', 'cd', 'segments'])['watch_time'].sum().reset_index()
df2['wt%'] = df2['watch_time'] / df2.groupby(['is_cricket', 'cd'])['watch_time'].transform(sum)
print(df2.to_csv('df3.csv', index=False))
