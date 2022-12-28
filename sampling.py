import pyspark.sql.functions as F
from pyspark.sql.types import BooleanType, ArrayType, StringType, IntegerType, StructType, StructField
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

valid_dates = match_df.select('date').distinct().toPandas()['date']
live_ads_inventory_forecasting_root_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting"
play_out_log_input_path = "s3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test"
# start_time = spark.read.parquet(f"{live_ads_inventory_forecasting_root_path}/break_start_time_data_of_{tournament}").toPandas()

test_dates = valid_dates[-3:] # small data for test
gen = (spark.read.csv(f"{play_out_log_input_path}/{d}", header=True) for d in test_dates)
playout_df = reduce(lambda x, y: x.union(y), gen).toPandas()

# check playout ID is unique under lang,tenant,platform
# playout_df[['Content ID', 'Playout ID', 'Language', 'Tenant', 'Platform']].drop_duplicates() \
#     .groupby(['Content ID', 'Language', 'Tenant', 'Platform']).aggregate(list)
# playout_gr['len'] = playout_gr['End Time'].apply(len)

wt = spark.read.parquet('s3://hotstar-dp-datalake-processed-us-east-1-prod/events/watched_video/cd=2022-11-13/hr=13/')
ext_cols = ['state', 'gender', 'city', 'region', 'pincode', 'device', 'partner_access', 'carrier', 'carrier_hs']
wt1 = wt[['content_id', 'dw_p_id', 'watch_time', 'timestamp', 'language', 'country', 'platform', 'stream_type', 'play_type', 'content_type', 'user_segments', 'subscription_status']]

playout_gr2 = playout_df.groupby(['Content ID', 'Playout ID']).aggregate({
    'End Time': list,
    'Language': max,
    'Tenant': max,
    'Platform': lambda s: max(s).split('|')
})
playout_gr2['endtime'] = playout_gr2['End Time'].apply(lambda s: sorted(pd.to_datetime(s)))
playout_gr2.reset_index(inplace=True)

# for (tournament, date, content id, playout id, Sr. No) x (user_segment)
@F.udf(returnType=ArrayType(StringType()))
def match(content_id, language, country, platform, end, watch_time):
    df = playout_gr2
    df2 = df[(df['Content ID'] == content_id)
                &(df.Language==language)
                &(df.Tenant==country)
                &(df.Platform.apply(lambda s: platform in s))]
    if len(df2) == 0:
        return []
    playout = df2.iloc[0]
    start = end - pd.Timedelta(seconds=watch_time)
    return [playout['Playout ID']] + \
        [str(i) for i, t in enumerate(playout.endtime) if start <= t <= end]

#@Unit Test
# row = wt1.head(1)[0]
# t=match('1540019068', row.language, row.country, row.platform, row.timestamp, row.watch_time)

wt2=wt1.withColumn('match', match('content_id', 'language', 'country', 'platform', 'timestamp', 'watch_time'))
wt2.show()
