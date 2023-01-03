import pandas as pd
import pyspark.sql.functions as F

output_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/sampling/'
df=spark.read.parquet(f'{output_path}cohort_agg/')

df2 = df.toPandas()
df3 = df2.groupby(['cd', 'content_id', 'cohort']).sum().reset_index()
top_inventory_ssai = 

print(
    df3.groupby(['cd', 'content_id']).ad_time.nlargest(3).reset_index(2, drop=True),
    df3.groupby(['cd', 'content_id']).reach.nlargest(3)
)
