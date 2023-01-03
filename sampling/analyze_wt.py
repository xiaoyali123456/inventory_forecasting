import json

import pandas as pd
import pyspark.sql.functions as F

output_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/sampling/'
playout_log_path = 's3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test/'
wt_path = 's3://hotstar-dp-datalake-processed-us-east-1-prod/events/watched_video/'
df=spark.read.parquet(f'{output_path}cohort_agg/cd=2022-11-13/')

df2 = df.toPandas()
df3 = df2.groupby(['content_id', 'cohort']).sum()
print(
    df3.groupby('content_id').ad_time.nlargest(3),
    df3.groupby('content_id').reach.nlargest(3)
)
