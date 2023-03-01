# To fix date format in watched_video
# PYSPARK_DRIVER_PYTHON_OPTS='-m IPython' pyspark --name inv_forecast --conf spark.sql.parquet.datetimeRebaseModeInRead=CORRECTED --conf spark.sql.parquet.int96RebaseModeInRead=CORRECTED

import json
import os
from datetime import datetime

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

output_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/inventory_v2/'
playout_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/playout_v3/' # v3 time is IST
watched_video_path = 's3://hotstar-ads-ml-us-east-1-prod/data_exploration/data/data_backup/watched_video/'

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

def load_playout_time(time_col):
    return time_col.map(lambda x: pd.Timestamp(x, tz='asia/kolkata').tz_convert('utc'))

def preprocess_playout(df):
    df = df.toPandas()
    df['break_start'] = load_playout_time(df['Start Time']) # XXX:hack fix for wrong
    df['break_end'] = load_playout_time(df['End Time'])
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
    playout_df = preprocess_playout(dt)
    playout_df2 = spark.createDataFrame(playout_df).where('platform != "na"')
    playout_df3 = playout_df2.where('platform == "na"').drop('platform')
    wt_path = f'{watched_video_path}cd={dt}/'
    wt = spark.read.parquet(wt_path) \
        .where(F.col("dw_p_id").substr(-1,1).isin(['2', 'a', 'e', '8']))
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
        .groupby('content_id', 'playout_id',
            'language', 'platform', 'country', 'cohort') \
        .agg(
            F.expr('sum(ad_time) as ad_time'),
            F.expr('count(distinct dw_d_id) as reach')
        ).repartition(16)
    wt4.write.mode('overwrite').parquet(final_path)
    print('end', datetime.now())

def main():
    tournaments=['wc2021', 'wc2022']
    for tour in tournaments:
        playout = spark.read.parquet(playout_path + tour)

        for dt in dates:
            process(tour, dt)

if __name__ == '__main__':
    main()
