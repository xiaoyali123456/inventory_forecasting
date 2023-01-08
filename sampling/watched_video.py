# PYSPARK_DRIVER_PYTHON_OPTS='-m IPython' pyspark --name minliang --conf spark.sql.parquet.datetimeRebaseModeInRead=CORRECTED --conf spark.sql.parquet.int96RebaseModeInRead=CORRECTED

import json
import os
from datetime import datetime

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import BooleanType, StringType

output_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/dw_d_id/'
playout_log_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/playout_v2/'
wt_root = 's3://hotstar-ads-ml-us-east-1-prod/data_exploration/data/data_backup/watched_video/'

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
                    'lower(title) as title', # XXX: must before where
                    'shortsummary') \
        .where(is_valid_title('title')) \
        .distinct()
    if save:
        match_df.write.mode('overwrite').parquet(f'{output_path}match_df/tournament={tournament}/')
    return match_df.select('date').distinct().toPandas()['date'] #TODO: this is UTC but playout is IST

def load_datetime(time_str):
    try:
        return pd.to_datetime(time_str)
    except:
        return None

def load_playout_time(date_col, time_col):
    ts = (date_col + ' ' + time_col).apply(load_datetime)
    return pd.Series(ts.dt.tz_localize('Asia/Kolkata').dt.tz_convert(None).dt.to_pydatetime(), dtype=object)

def prepare_playout_df(dt):
    df = spark.read.csv(f'{playout_log_path}cd={dt}', header=True).toPandas()
    # df['break_start'] = load_playout_time(df['Start Date'], df['Start Time'])
    # df['break_end'] = load_playout_time(df['End Date'], df['End Time'])
    df['break_start'] = load_playout_time(dt, df['Start Time']) # hack fix for wrong
    df['break_end'] = load_playout_time(dt, df['End Time'])
    df = df[~(df.break_start.isna()|df.break_end.isna())]
    df.rename(columns={
        'Content ID': 'content_id',
        'Playout ID': 'playout_id',
        'Language': 'language',
        'Tenant': 'country',
        'Platform': 'platform',
    }, inplace=True)
    df.platform = df.platform.str.split('|')
    df = df.explode('platform')
    df[['playout_id', 'content_id', 'language', 'country', 'platform']] = \
        df[['playout_id', 'content_id', 'language', 'country', 'platform']].applymap(str.strip)
    df[['language', 'platform']] = df[['language', 'platform']].applymap(str.lower)
    return df[['content_id', 'playout_id', 'language', 'country', 'platform', 'break_start', 'break_end']]

def process(tournament, dt):
    print('process', dt)
    print('begin', datetime.now())
    final_path = f'{output_path}cohort_agg_quarter/tournament={tournament}/cd={dt}/'
    success_path = f'{final_path}_SUCCESS'
    if os.system('aws s3 ls ' + success_path) == 0:
        return
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
    wt4 = wt3.where('ad_time > 0') \
        .withColumn('cohort', parse('user_segments')) \
        .groupby('content_id', 'playout_id', 'cohort') \
        .agg(
            F.expr('sum(ad_time) as ad_time'),
            F.expr('count(distinct dw_d_id) as reach')
        ).repartition(16)
    wt4.write.mode('overwrite').parquet(final_path)
    print('end', datetime.now())

def main():
    tournaments=['wc2021'] #'wc2022', 'ipl2021',
    # aws s3 sync s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/valid_dates/ .
    for tournament in tournaments:
        cache = tournament+'.json'
        if os.path.exists(cache):
            print('dates of', tournament, 'exist')
            with open(cache) as f:
                dates = json.load(f)
        else:
            dates = sorted(valid_dates(tournament))
            with open(cache, 'w') as f:
                json.dump(dates, f)
        for dt in dates:
            process(tournament, dt)

if __name__ == '__main__':
    main()
