import pandas as pd
from common import *

out = 'sub_vv_2.csv'
AU_table = 'data_warehouse.watched_video_daily_aggregates_ist'

try:
    df = pd.read_csv(out)
except:
    df = pd.DataFrame({'ds':[], 'vv': [], 'sub_vv': []})

# for i in pd.date_range('2022-06-01', '2023-03-01'):
for i in pd.date_range('2023-03-02', '2023-04-11'):
    ds = str(i.date())
    if ds not in set(df.ds):
        print(ds)
        sql = f'select dw_p_id from {AU_table} where cd = "{ds}"'
        wv = spark.sql(sql)
        sub_wv = spark.sql(sql + ' and lower(subscription_status) in ("active", "cancelled", "graceperiod")')
        tmp = pd.DataFrame({
            'ds': [ds],
            'vv': [wv.distinct().count()],
            'sub_vv': [sub_wv.distinct().count()],
        })
        df = pd.concat([df, tmp])
        if len(df) % 30 == 0:
            df.to_csv(out, index=False)

df.to_csv(out, index=False)
!aws s3 cp sub_vv_2.csv s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_full_v2/sub_vv_3.csv # type: ignore

# out = 'sub_vv_2.csv'
# df = pd.read_csv(out)
# df2 = pd.read_parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_full_v2/all/')
# df3 = pd.concat([df2, df])
# df3.to_parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_full_v2/all/cd=2023-04-11/part0.parquet')
