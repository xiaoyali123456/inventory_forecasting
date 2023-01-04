import json
import os
from datetime import datetime

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import BooleanType, FloatType, StringType

output_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/sampling/'
playout_log_path = 's3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test/'
wt_path = 's3://hotstar-dp-datalake-processed-us-east-1-prod/events/watched_video/'

@F.udf(returnType=BooleanType())
def is_valid_title(title):
    for arg in ['warm-up', 'follow on']:
        if arg in title:
            return False
    return ' vs ' in title

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

def valid_dates(tournament, save=False):
    match_meta_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/data/ads_crash/match_meta'
    tournament_dic = {'wc2022': 'ICC Men\'s T20 World Cup 2022',
                    'ipl2022': 'TATA IPL 2022',
                    'wc2021': 'ICC Men\'s T20 World Cup 2021',
                    'ipl2021': 'VIVO IPL 2021'}
    match_df = spark.read.parquet(match_meta_path) \
        .where(f'shortsummary="{tournament_dic[tournament]}" and contenttype="SPORT_LIVE"') \
        .selectExpr('substring(from_unixtime(startdate), 1, 10) as date',
                    'contentid as content_id',
                    'lower(title) as title',
                    'shortsummary') \
        .where(is_valid_title('title')) \
        .orderBy('date') \
        .distinct()
    if save:
        match_df.write.mode('overwrite').parquet(f'{output_path}match_df/tournament={tournament}/')
    return match_df.select('date').distinct().toPandas()['date'] #TODO: this is UTC but playout is IST

def load_playout_time(date_col, time_col):
    ts = (date_col + ' ' + time_col).apply(pd.to_datetime)
    return pd.Series(ts.dt.tz_localize('Asia/Kolkata').dt.tz_convert(None).dt.to_pydatetime(), dtype=object)

def check_ist_utc_problem(dt):
    df = spark.read.csv(f'{playout_log_path}{dt}', header=True).toPandas()
    print('check', dt)
    try:
        print(min((df['Start Date'] + ' ' + df['Start Time']).apply(pd.to_datetime)))
        print(max((df['End Date'] + ' ' + df['End Time']).apply(pd.to_datetime)))
    except Exception as e:
        print(e)

def prepare_playout_df(dt):
    playout_df = spark.read.csv(f'{playout_log_path}{dt}', header=True).toPandas()
    playout_df['break_start'] = load_playout_time(playout_df['Start Date'], playout_df['Start Time'])
    playout_df['break_end'] = load_playout_time(playout_df['End Date'], playout_df['End Time'])
    playout_df['Platform'] = playout_df['Platform'].apply(lambda s: s.split('|'))
    playout_df = playout_df.explode('Platform')
    playout_df.rename(columns={
        'Content ID': 'content_id',
        'Playout ID': 'playout_id',
        'Language': 'language',
        'Tenant': 'country',
        'Platform': 'platform',
    }, inplace=True)
    playout_df.language = playout_df.language.str.lower()
    return playout_df[['content_id', 'playout_id', 'language', 'contry', 'platform', 'break_start', 'break_end']]

def main():
    tournament='wc2022'
    dates = list(valid_dates(tournament))
    dates.remove('2022-10-17') #TODO: dirty hack; wrong data
    for dt in dates:
        print('process', dt)
        final_path = f'{output_path}cohort_agg/cd={dt}/'
        success_path = f'{final_path}_SUCCESS'
        if os.system('aws s3 ls ' + success_path) == 0:
            continue
        playout_df = prepare_palyout_df(dt)
        playout_df2 = spark.createDataFrame(playout_df)
        playout_df3 = playout_df2.where('platform == "na"').drop('platform')
        wt = spark.read.parquet(f'{wt_path}cd={dt}/')
        wt1 = wt[['dw_p_id', 'content_id', 'watch_time', 'timestamp', 'country',
            F.expr('lower(language) as language'),
            F.expr('lower(platform) as platform'),
            'user_segments']] \
            .where('substring(dw_p_id, 1, 1) < "4"') \
            .repartition(2048, 'content_id')
        cache_path = f'{output_path}cohort_agg_cache_wt/'
        wt1.write.mode('overwrite').parquet(cache_path)
        print('wt1', datetime.now())
        wt1=spark.read.parquet(cache_path)
        wt2a = wt1.join(F.broadcast(playout_gr2), on=['content_id', 'language', 'country', 'platform'])
        wt2b = wt1.join(F.broadcast(playout_gr3), on=['content_id', 'language', 'country'])[wt2a.columns] # reorder cols for union
        wt2 =  wt2a.union(wt2b)
        cache2_path = f'{output_path}cohort_agg_cache2_wt/'
        wt2.write.mode('overwrite').parquet(cache2_path)
        print('wt2', datetime.now())
        wt2=spark.read.parquet(cache_path)
        wt3 = wt2.withColumn('ad_time', intersect('timestamp', 'watch_time', 'break_start', 'break_end'))
        cache3_path = f'{output_path}cohort_agg_cache_wt3/'
        wt3.write.mode('overwrite').parquet(cache3_path) # 8 min on 32 m5.xlarge
        print('wt3', datetime.now())
        wt3=spark.read.parquet(cache3_path)
        wt4 = wt3.where('ad_time > 0') \
            .withColumn('cohort', parse('user_segments')) \
            .groupby('content_id', 'playout_id', 'cohort') \
            .agg(
                F.sum(F.col('ad_time')).alias('ad_time'),
                F.countDistinct(F.col('dw_p_id')).alias('reach')
            ).repartition(16)
        wt4.write.mode('overwrite').parquet(final_path)
        print('wt4', datetime.now())

if __name__ == '__main__':
    main()
