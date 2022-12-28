import pyspark.sql.functions as F
from pyspark.sql.types import BooleanType
from functools import reduce
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

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

test_dates = valid_dates[-3:] # small data for test
gen = (spark.read.csv(f"{play_out_log_input_path}/{d}", header=True) for d in test_dates)
playout_df = reduce(lambda x, y: x.union(y), gen).toPandas()
# End Time - Start Time = Delivered time <= Actual Time
# (pd.to_datetime(playout_df['End Time']) - pd.to_datetime(playout_df['Start Time']) - playout_df['Delivered Time'].apply(pd.Timedelta)).describe()
# (playout_df['Delivered Time'].apply(pd.Timedelta) / playout_df['Actual Time'].apply(pd.Timedelta)).describe()

playout_gr = playout_df.groupby(['Content ID', 'Playout ID', 'Break ID']).aggregate(list)
playout_gr['break_diff'] = playout_gr['End Time'].apply(pd.to_datetime).apply(lambda t:max(t)-min(t))
playout_gr['break_mean'] = playout_gr['End Time'].apply(pd.to_datetime).apply(lambda x:x.mean())
playout_gr['break_mean_diff'] = playout_gr.sort_values(['Content ID', 'Playout ID', 'break_mean']).groupby(['Content ID', 'Playout ID'])['break_mean'].diff()
playout_gr['break_max'] = playout_gr['End Time'].apply(pd.to_datetime).apply(max)

print(playout_gr.break_diff.quantile(np.arange(0,1.1,0.1)))
print(playout_gr.break_mean_diff.quantile(np.arange(0,1.1,0.1)))

# check playout ID is unique under lang,tenant,platform
# playout_df[['Content ID', 'Playout ID', 'Language', 'Tenant', 'Platform']].drop_duplicates() \
#     .groupby(['Content ID', 'Language', 'Tenant', 'Platform']).aggregate(list)
# playout_gr['len'] = playout_gr['End Time'].apply(len)

wt = spark.read.parquet('s3://hotstar-dp-datalake-processed-us-east-1-prod/events/watched_video/cd=2022-11-13/hr=13/')
ext_cols = ['state', 'gender', 'city', 'region', 'pincode', 'device', 'partner_access', 'carrier', 'carrier_hs']
wt1 = wt[['content_id', 'dw_p_id', 'watch_time', 'timestamp', 'language', 'country', 'platform', 'stream_type', 'play_type', 'content_type', 'user_segments', 'subscription_status']]

content = pd.unique(playout_df['Content ID'])
query = ",".join(f'"{c}"' for c in content)
wt2 = wt1.where(f'content_id in ({query})')

playout_gr2 = playout_df.groupby(['Content ID', 'Playout ID']).aggregate(list)
playout_gr2['endtime']=playout_gr2['End Time'].apply(pd.to_datetime(s))

# playout_gr2['endtime_hm']=playout_gr2.endtime.map(lambda s: [x.replace(second=0) for x in s])
# plt.close()
# for i in range(4):
#     plt.hist(playout_gr2.endtime_hm[i], bins=150, alpha=0.7,
#         label='+'.join([playout_gr2['Language'][i][0], playout_gr2['Tenant'][i][0], playout_gr2['Platform'][i][0]]))
# plt.legend()
# plt.savefig('test.png')
