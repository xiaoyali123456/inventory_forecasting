# compare midroll reach/inventory distribution

import pandas as pd
at = pd.read_parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_v2/ad_time/cd=2023-07-10/')
re = pd.read_parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_v2/reach/cd=2023-07-10/')
at['r'] =at.ad_time/at.ad_time.sum()
re['r']=re.reach/re.reach.sum()

cols =re.columns[:-2]
df = at.merge(re, on =cols)
df = at.merge(re, on =cols.tolist())

df.sort_values('r_x', ascending=False)
print((df.r_x/df.r_y).describe())