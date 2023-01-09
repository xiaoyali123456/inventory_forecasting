import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import BooleanType, StringType

output_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/dw_d_id/'
playout_log_path = 's3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test/'
wt_root = 's3://hotstar-dp-datalake-processed-us-east-1-prod/events/watched_video/'

def load_datetime(time_str):
    try:
        return pd.to_datetime(time_str)
    except:
        return None

def load_playout_time(date_col, time_col):
    ts = (date_col + ' ' + time_col).apply(load_datetime)
    return pd.Series(ts.dt.tz_localize('Asia/Kolkata').dt.tz_convert(None).dt.to_pydatetime(), dtype=object)


def prepare_playout_df(dt):
    playout_df = spark.read.csv(f'{playout_log_path}{dt}', header=True).toPandas()
    playout_df['break_start'] = load_playout_time(playout_df['Start Date'], playout_df['Start Time'])
    playout_df['break_end'] = load_playout_time(playout_df['End Date'], playout_df['End Time'])
    playout_df = playout_df[~(playout_df.break_start.isna()|playout_df.break_end.isna())]
    playout_df.rename(columns={
        'Content ID': 'content_id',
        'Playout ID': 'playout_id',
        'Language': 'language',
        'Tenant': 'country',
        'Platform': 'platform',
    }, inplace=True)
    playout_df.language = playout_df.language.str.lower()
    playout_df.platform = playout_df.platform.str.split('|')
    playout_df = playout_df.explode('platform')
    return playout_df[['content_id', 'playout_id', 'language', 'country', 'platform', 'break_start', 'break_end']]

dates = [
  "2022-10-16",
  "2022-10-17",
  "2022-10-18",
  "2022-10-19",
  "2022-10-20",
  "2022-10-21",
  "2022-10-22",
  "2022-10-23",
  "2022-10-24",
  "2022-10-25",
  "2022-10-26",
  "2022-10-27",
  "2022-10-28",
  "2022-10-29",
  "2022-10-30",
  "2022-10-31",
  "2022-11-01",
  "2022-11-02",
  "2022-11-03",
  "2022-11-04",
  "2022-11-05",
  "2022-11-06",
  "2022-11-09",
  "2022-11-10",
  "2022-11-13"
]

for dt in dates:
    df = prepare_playout_df(dt)
    for c in set(df.content_id):
        if ' ' in c:
            print(dt, f'#{c}#')
            break
    for c in set(df.language):
        if ' ' in c:
            print(dt, f'#{c}#')
            break
    for c in set(df.platform):
        if ' ' in c:
            print(dt, f'#{c}#')
            break
    for c in set(df.country):
        if ' ' in c:
            print(dt, f'#{c}#')
            break
#########

def prepare_playout_df(dt):
    playout_df = spark.read.csv(f'{playout_log_path}{dt}', header=True).toPandas()
    playout_df['break_start'] = load_playout_time(playout_df['Start Date'], playout_df['Start Time'])
    playout_df['break_end'] = load_playout_time(playout_df['End Date'], playout_df['End Time'])
    playout_df = playout_df[~(playout_df.break_start.isna()|playout_df.break_end.isna())]
    playout_df.rename(columns={
        'Content ID': 'content_id',
        'Playout ID': 'playout_id',
        'Language': 'language',
        'Tenant': 'country',
        'Platform': 'platform',
    }, inplace=True)
    playout_df.content_id = playout_df.content_id.str.strip()
    playout_df.language = playout_df.language.str.lower()
    playout_df.platform = playout_df.platform.str.split('|')
    playout_df = playout_df.explode('platform')
    return playout_df[['content_id', 'playout_id', 'language', 'country', 'platform', 'break_start', 'break_end']]

dt='2022-10-16'
final_path = f'{output_path}cohort_agg/cd={dt}/'
success_path = f'{final_path}_SUCCESS'
playout_df = prepare_playout_df(dt)
playout_df2 = spark.createDataFrame(playout_df).where('platform != "na"')
playout_df3 = playout_df2.where('platform == "na"').drop('platform')
wt_path = f'{wt_root}cd={dt}/'
wt = spark.read.parquet(wt_path)
# TODO: use received_at if received_at < timestamp
wt1 = wt[['dw_d_id', 'content_id', 'timestamp', 'country', 'user_segments',
    F.expr('lower(language) as language'),
    F.expr('lower(platform) as platform'),
    F.expr('timestamp - make_interval(0,0,0,0,0,0,watch_time) as start_timestamp')
]]

wt2a = wt1.join(playout_df2.hint('broadcast'), on=['content_id', 'language', 'platform', 'country'])
wt2b = wt1.join(playout_df3.hint('broadcast'), on=['content_id', 'language', 'country'])[wt2a.columns]
wt2 = wt2a.union(wt2b)
wt3 = wt2.withColumn('ad_time', 
    F.expr('bigint(least(timestamp, break_end) - greatest(start_timestamp, break_start))'))

wt3.write.parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/debug_wt3/')
wt4=spark.read.parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/debug_wt3/')
wt4.where('content_id == "1540018945"').show() # nothing

# debug
h=wt1.where('content_id == "1540018945"')[['country', 'language', 'platform']].distinct().toPandas()

df=spark.read.parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/dw_d_id/cohort_agg/cd=2022-10-16/').toPandas()
df[df.content_id == "1540018945"]
