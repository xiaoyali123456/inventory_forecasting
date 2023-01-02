import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, TimestampType, IntegerType
from pyspark.sql.types import BooleanType, StringType, FloatType
from functools import reduce
import pandas as pd
import numpy as np
import json

root_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/sampling/"
play_out_log_input_path = "s3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test/"
wt_path = 's3://hotstar-dp-datalake-processed-us-east-1-prod/events/watched_video/'

def uniq_check(df):
    df2 = df[['content_id', 'language', 'country', 'platform']].drop_duplicates()
    assert len(df) == len(df2)
    na_df = df[df.platform == 'na']
    na_df2 = pd.merge(df[df.platform != 'na'], na_df, on=['content_id', 'language', 'country'])
    assert len(na_df2) == 0

@F.udf(returnType=BooleanType())
def is_valid_title(title):
    for arg in ['warm-up', 'follow on']:
        if arg in title:
            return False
    return ' vs ' in title

@F.udf(returnType=FloatType())
def intersect(end, watch_time, break_start, break_end):
    start = end - pd.Timedelta(seconds=watch_time)
    sm = 0.0
    for s, e in zip(break_start, break_end):
        inter = (min(e, end) - max(s, start)).total_seconds()
        sm += max(inter, 0)
    return sm

@F.udf(returnType=StringType())
def parse(segments):
    try:
        js = json.loads(segments)
        if type(js) == list:
            for s in js:
                if s.startswith('SSAI::'):
                    return s
        elif type(js) == dict:
            return js.get('ssaiTag')
    except:
        pass

def valid_dates(tournament):
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
        .distinct()
    return match_df.select('date').distinct().toPandas()['date'] #TODO: this is UTC but playout is IST

def load_playout_time(date_col, time_col):
    ts = (date_col + ' ' + time_col).apply(pd.to_datetime)
    return pd.Series(ts.dt.tz_localize('Asia/Kolkata').dt.tz_convert(None).dt.to_pydatetime(), dtype=object)

def main():
    tournament='wc2022'
    dates = valid_dates(tournament)
    dt = dates.iloc[-1]
    playout_df = spark.read.csv(f"{play_out_log_input_path}{dt}", header=True).toPandas()
    playout_df['break_start'] = load_playout_time(playout_df['Start Date'], playout_df['Start Time'])
    playout_df['break_end'] = load_playout_time(playout_df['End Date'], playout_df['End Time'])
    playout_gr = playout_df.groupby(['Content ID', 'Playout ID']).aggregate({
        'Language': max,
        'Tenant': max,
        'Platform': lambda s: max(s).split('|'),
        'break_start': lambda s: sorted(s),
        'break_end': lambda s: sorted(s),
    })
    playout_gr = playout_gr.explode('Platform')
    playout_gr.reset_index(inplace=True)
    playout_gr.rename(columns={
        'Content ID': 'content_id',
        'Playout ID': 'playout_id',
        'Language': 'language',
        'Tenant': 'country',
        'Platform': 'platform',
    }, inplace=True)
    playout_gr.language = playout_gr.language.str.lower()
    uniq_check(playout_gr)
    playout_gr2 = spark.createDataFrame(playout_gr)
    playout_gr3 = playout_gr2.where('platform == "na"').drop('platform')

    wt = spark.read.parquet(f'{wt_path}cd={dt}/hr=13/')
    # ext_cols = ['state', 'gender', 'city', 'region', 'pincode', 'device',
    #  'stream_type', 'play_type', 'content_type', 'subscription_status', 'video_position',
    #  'partner_access', 'carrier', 'carrier_hs']
    wt1 = wt[['dw_p_id', 'content_id', 'watch_time', 'timestamp', 'country',
        'stream_type', 'play_type', 'content_type', 'subscription_status',
        F.expr('lower(language) as language'),
        F.expr('lower(platform) as platform'),
        'user_segments']]
    wt2a = wt1.join(playout_gr2, on=['content_id', 'language', 'country', 'platform'])
    wt2b = wt1.join(playout_gr3, on=['content_id', 'language', 'country'])[wt2a.columns] # reorder cols for union
    wt2 =  wt2a.union(wt2b)
    wt2.write.mode("overwrite").parquet('s3://hotstar-ads-ml-us-east-1-prod/tmp/minliang/sampling_wt2/')
    wt3 = wt2.withColumn('ad_time', intersect('timestamp', 'watch_time', 'break_start', 'break_end'))
    # wt3.write.mode("overwrite").parquet('s3://hotstar-ads-ml-us-east-1-prod/tmp/minliang/sampling_wt3/')
    # wt3 = spark.read.parquet('s3://hotstar-ads-ml-us-east-1-prod/tmp/minliang/sampling_wt3/')
    wt4 = wt3.withColumn('cohort', parse('user_segments')) \
        .groupby('content_id', 'playout_id', 'cohort') \
        .sum('ad_time').withColumnRenamed('sum(ad_time)', 'inventory')
    wt4.write.parquet('s3://hotstar-ads-ml-us-east-1-prod/tmp/minliang/sampling_wt4/')

main()
