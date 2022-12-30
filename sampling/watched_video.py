import pyspark.sql.functions as F
from pyspark.sql.types import BooleanType, ArrayType, TimestampType, StringType
from functools import reduce
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
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

@F.udf(returnType=ArrayType(TimestampType()))
def match(end, watch_time, break_end):
    start = end - pd.Timedelta(seconds=watch_time)
    return [t for t in break_end if start <= t <= end]

@F.udf(returnType=ArrayType(StringType()))
def parse(seg):
    try:
        js = json.loads(seg)
        if type(js) == list:
            for s in js:
                if s.startswith('SSAI::'):
                    tag = s
                    break
        elif type(js) == dict:
            if 'ssaiTag' in js:
                tag=js['ssaiTag']
            else:
                return []
        else:
            return []
        return tag[6:].split(':')
    except:
        return []

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
    valid_dates = match_df.select('date').distinct().toPandas()['date'] #TODO: this is UTC but playout is IST
    return valid_dates

def main(tournament='wc2022'):
    test_dates = valid_dates()#[-3:] # small data for test
    gen = (spark.read.csv(f"{play_out_log_input_path}{d}", header=True) for d in test_dates)
    playout_df = reduce(lambda x, y: x.union(y), gen).toPandas()

    wt = spark.read.parquet(f'{wt_path}cd=2022-11-13/hr=13/')
    ext_cols = ['state', 'gender', 'city', 'region', 'pincode', 'device', 'partner_access', 'carrier', 'carrier_hs']
    wt1 = wt[['dw_p_id', 'content_id', 'watch_time', 'timestamp', 'country',
        F.expr('lower(language) as language'),
        F.expr('lower(platform) as platform'),
        'stream_type', 'play_type', 'content_type', 'user_segments', 'subscription_status']]
    # wt1 = wt1.where('substring(dw_p_id, 1, 1) == "0"') # 1/16 sampling
    # wt1 = wt1.replace('FireStick', 'firetv', 'platform').replace('webos', 'web', 'platform')

    playout_df['break_end'] = (playout_df['End Date'] + ' ' + playout_df['End Time']).apply(pd.to_datetime)
    playout_df['break_end'] = pd.Series(playout_df.break_end.dt.tz_localize('Asia/Kolkata').dt.to_pydatetime(), dtype=object)
    playout_gr = playout_df.groupby(['Content ID', 'Playout ID']).aggregate({
        'Language': max,
        'Tenant': max,
        'Platform': lambda s: max(s).split('|'),
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

    wt2a = wt1.join(playout_gr2, on=['content_id', 'language', 'country', 'platform'])
    wt2b = wt1.join(playout_gr3, on=['content_id', 'language', 'country'])[wt2a.columns] # reorder cols for union
    wt2 =  wt2a.union(wt2b)
    wt3 = wt2.withColumn('match_break', match('timestamp', 'watch_time', 'break_end')).cache()
    wt3.write.mode("overwrite").parquet('s3://hotstar-ads-ml-us-east-1-prod/tmp/minliang/sampling_wt3/')
    wt3 = spark.read.parquet('s3://hotstar-ads-ml-us-east-1-prod/tmp/minliang/sampling_wt3/')

    wt4 = wt3.where('size(match_break) > 0') \
        .withColumn('match_break', F.explode('match_break')) \
        .withColumn('cohort', F.explode(parse('user_segments'))) \
        .groupby('content_id', 'playout_id', 'match_break', 'cohort') \
        .count()
    wt4.write.parquet('s3://hotstar-ads-ml-us-east-1-prod/tmp/minliang/sampling_match_cohort_count/')
    wt5 = spark.read.parquet('s3://hotstar-ads-ml-us-east-1-prod/tmp/minliang/sampling_match_cohort_count/').toPandas()

