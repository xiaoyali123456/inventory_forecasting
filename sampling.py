import pyspark.sql.functions as F
from pyspark.sql.types import BooleanType
from functools import reduce
import pandas as pd
import numpy as np

@F.udf(returnType=BooleanType())
def is_valid_title(title):
    for arg in ['warm-up', 'follow on']:
        if arg in title:
            return False
    return ' vs ' in title

tournament='wc2022'
match_meta_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/ads_crash/match_meta"
tournament_dic = {"wc2022": "ICC Men\'s T20 World Cup 2022",
                  "ipl2022": "TATA IPL 2022",
                  "wc2021": "ICC Men\'s T20 World Cup 2021",
                  "ipl2021": "VIVO IPL 2021"}

match_df = spark.read.parquet(match_meta_path) \
    .where(f'shortsummary="{tournament_dic[tournament]}" and contenttype="SPORT_LIVE"') \
    .where(is_valid_title('title')) \
    .selectExpr('substring(from_unixtime(startdate), 1, 10) as date',
                'contentid as content_id',
                'lower(title) as title',
                'shortsummary') \
    .orderBy('date') \
    .distinct() \
    .cache()
# match_df.unpersist()

valid_dates = match_df.select('date').distinct().toPandas()['date']
live_ads_inventory_forecasting_root_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting"
play_out_log_input_path = "s3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test"
# start_time = spark.read.parquet(f"{live_ads_inventory_forecasting_root_path}/break_start_time_data_of_{tournament}").toPandas()

test_dates = valid_dates[-1:] # small data for test
gen = (spark.read.csv(f"{play_out_log_input_path}/{d}", header=True) for d in test_dates)
playout_df = reduce(lambda x, y: x.union(y), gen).toPandas()
# End Time - Start Time = Delivered time <= Actual Time
# (pd.to_datetime(playout_df['End Time']) - pd.to_datetime(playout_df['Start Time']) - playout_df['Delivered Time'].apply(pd.Timedelta)).describe()
# (playout_df['Delivered Time'].apply(pd.Timedelta) / playout_df['Actual Time'].apply(pd.Timedelta)).describe()

playout_gr = playout_df.groupby(['Content ID', 'Playout ID', 'Break ID']).aggregate(list)
playout_gr['break_diff'] = playout_gr['End Time'].apply(pd.to_datetime).apply(lambda t:max(t)-min(t))
playout_gr['break_mean'] = playout_gr['End Time'].apply(pd.to_datetime).apply(lambda x:x.mean())
playout_gr['break_mean_diff'] = playout_gr.sort_values(['Content ID', 'Playout ID', 'break_mean']).groupby(['Content ID', 'Playout ID'])['break_mean'].diff()

print(playout_gr.break_diff.quantile(np.arange(0,1.1,0.1)))
print(playout_gr.break_mean_diff.quantile(np.arange(0,1.1,0.1)))

playout = language, tenant, platform
watch_time = language, tenant, platform
