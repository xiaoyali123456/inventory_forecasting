# PYSPARK_DRIVER_PYTHON_OPTS='-m IPython' pyspark --name minliang
import json
import os
from datetime import datetime

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

output_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/inventory_wt/'
playout_log_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/playout_log_v2/wc2019/'
wt_root = 's3://hotstar-ads-ml-us-east-1-prod/data_exploration/data/data_backup/watched_video/'

def ist_to_utc(date, ist_time):
    return (date + ' ' + ist_time).apply(
        lambda x: pd.Timestamp(x, tz='asia/kolkata')
        .tz_convert('utc'))

@F.udf(returnType=StringType())
def parse(segments):
    try:
        js = json.loads(segments)
        if isinstance(js, list):
            for s in js:
                if s.startswith('SSAI::'):
                    return s
        elif isinstance(js, dict):
            return js.get('ssaiTag')
    except:
        pass

def prepare_playout_df(dt):
    df = spark.read.parquet(playout_log_path).toPandas()
    df['break_start'] = ist_to_utc(df.break_ist_date, df.break_ist_start_time)
    df = df[(df.break_start.apply(lambda x: str(x.date())) == dt)&
        ~df.duration.isna()]
    df['break_end'] = df.break_start + \
        df.duration.apply(lambda x: pd.Timedelta(seconds=x))
    df['country'] = df['tenant'].str.slice(0, 2)
    df[['content_id', 'language', 'country', 'platform']] = \
        df[['content_id', 'language', 'country', 'platform']].applymap(str.strip)
    df[['language', 'platform']] = df[['language', 'platform']].applymap(str.lower)
    return df[['content_id', 'language', 'country', 'break_start', 'break_end']]

def process(tournament, dt):
    print('process', dt)
    print('begin', datetime.now())
    final_path = f'{output_path}cohort_agg_quarter/tournament={tournament}/cd={dt}/'
    success_path = f'{final_path}_SUCCESS'
    if os.system('aws s3 ls ' + success_path) == 0:
        return
    playout_df = prepare_playout_df(dt)
    playout_df2 = spark.createDataFrame(playout_df)
    wt = spark.read.parquet(f'{wt_root}cd={dt}/')
    wt1 = wt[['dw_d_id', 'content_id', 'timestamp', 'platform', 'user_segments',
        F.expr('lower(country) as country'),
        F.expr('lower(language) as language'),
        F.expr('timestamp - make_interval(0,0,0,0,0,0,watch_time) as start_timestamp')
    ]]
    wt2 = wt1.join(playout_df2.hint('broadcast'), on=['content_id', 'language', 'country'])
    wt3 = wt2.withColumn('ad_time', 
        F.expr('bigint(least(timestamp, break_end) - greatest(start_timestamp, break_start))'))
    wt4 = wt3.where('ad_time > 0') \
        .withColumn('cohort', parse('user_segments')) \
        .groupby('content_id', 'language', 'platform', 'country', 'cohort') \
        .agg(
            F.expr('sum(ad_time) as ad_time'),
            F.expr('count(distinct dw_d_id) as reach')
        ).repartition(16)
    wt4.write.mode('overwrite').parquet(final_path)
    print('end', datetime.now())

def main():
    playout = spark.read.parquet(playout_log_path).toPandas()
    t = ist_to_utc(playout.break_ist_date, playout.break_ist_start_time)
    dates = set([str(x.date()) for x in t])
    tournaments=['wc2019']
    for tournament in tournaments:
        for dt in dates:
            process(tournament, dt)

if __name__ == '__main__':
    main()
