import pandas as pd

out_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/custom_cohort_inventory_v1/'
df = spark.read.parquet(out_path).toPandas()
df2 = df[df.country == 'in'].groupby(['is_cricket', 'cd', 'segments'])['watch_time'].sum().reset_index()
df2['wt%'] = df2['watch_time'] / df2.groupby(['is_cricket', 'cd'])['watch_time'].transform(sum)

# with pd.option_context('display.max_rows', 200):
#     display(df2)

print(df2.to_csv(index=False))