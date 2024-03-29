# To fix date format in watched_video
# PYSPARK_DRIVER_PYTHON_OPTS='-m IPython' pyspark --name inv_forecast --conf spark.sql.parquet.datetimeRebaseModeInRead=CORRECTED --conf spark.sql.parquet.int96RebaseModeInRead=CORRECTED

import json
import os
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql.types import StringType

out_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/inventory_v2/'
playout_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/playout_v3/' # v3 time is IST
watched_video_path = 's3://hotstar-ads-ml-us-east-1-prod/data_exploration/data/data_backup/watched_video/'

@F.udf(returnType=StringType())
def parse(segments):
    if segments is None:
        return None
    try:
        js = json.loads(segments)
    except:
        return None
    if type(js) == list:
        lst = js
    elif type(js) == dict:
        lst =js.get('data', [])
    else:
        return None
    filtered = set()
    equals = ['A_15031263', 'A_94523754', 'A_40990869', 'A_21231588', # device price
              'A_34365936', 'A_49094287', 'AP_107', 'AP_2AS', 'AP_2AT'] # sponsor custom cohort
    prefixs = ['NCCS_', 'CITY_', 'STATE_', 'SSAI::', 'FMD00', 'MMD00', 'P_']
    middles = ['_MALE_', '_FEMALE_']
    for t in lst:
        match = False
        for s in equals:
            if t == s:
                match = True
                break
        if not match:
            for s in prefixs:
                if t.startswith(s):
                    match = True
                    break
        if not match:
            for s in middles:
                if s in t:
                    match = True
                    break
        if match:
            filtered.add(t)
    return '|'.join(sorted(filtered))

def preprocess_playout(df):
    return df.selectExpr(
        'content_id',
        'trim(lower(playout_id)) as playout_id',
        'trim(lower(content_language)) as language',
        'trim(lower(tenant)) as country',
        'explode(split(trim(lower(platform)), "\\\\|")) as platform',
        'to_utc_timestamp(start_time, "IST") as break_start',
        'to_utc_timestamp(end_time, "IST") as break_end',
    ).where(
        'break_start is not null and break_end is not null'
    )

def process(tournament, dt, playout):
    print('process', dt)
    print('begin', datetime.now())
    out_table_path = f'{out_path}distribution_of_quarter_data/tournament={tournament}/cd={dt}/'
    success_path = f'{out_table_path}_SUCCESS'
    if os.system('aws s3 ls ' + success_path) == 0:
        return
    playout1 = spark.createDataFrame(playout) \
        .withColumn('break_start', F.expr('cast(break_start as long)')) \
        .withColumn('break_end', F.expr('cast(break_end as long)'))
    playout2 = playout1.where('platform != "na"')
    playout3 = playout2.where('platform == "na"').drop('platform')
    wt_path = f'{watched_video_path}cd={dt}/'
    wt = spark.read.parquet(wt_path)
    # debug = True
    debug = False
    if debug:
        wt = spark.read.parquet('s3://hotstar-ads-ml-us-east-1-prod/data_exploration/data/data_backup/watched_video/cd=2022-11-05/part-00000-f1c86119-d4af-4f70-bec2-fabbc27ff391-c000.snappy.parquet')
        out_table_path = f'{out_path}debug_out_table/'
    # TODO: use received_at if received_at < timestamp
    wt1 = wt[['dw_d_id', 'content_id', 'user_segments',
        # 'manufacturer', 'model', 'device', 'model_code',
        F.expr('lower(language) as language'),
        F.expr('lower(platform) as platform'),
        F.expr('lower(country) as country'),
        F.expr('lower(city) as city'),
        F.expr('lower(state) as state'),
        F.expr('cast(timestamp as long) as end'),
        F.expr('cast(timestamp as double) - watch_time as start'),
    ]]
    wt2a = wt1.join(playout2.hint('broadcast'), on=['content_id', 'language', 'platform', 'country'])
    wt2b = wt1.join(playout3.hint('broadcast'), on=['content_id', 'language', 'country'])[wt2a.columns]
    wt2 = wt2a.union(wt2b)
    wt3 = wt2.withColumn('ad_time', F.expr('least(end, break_end) - greatest(start, break_start)'))
    npar = 32
    if debug:
        npar = 1
    wt4 = wt3.where('ad_time > 0') \
        .withColumn('cohort', parse('user_segments')) \
        .groupby('content_id', 'playout_id',
            'language', 'platform', 'country',
            'city', 'state', 'cohort') \
        .agg(
            F.expr('sum(ad_time) as ad_time'),
            F.expr('count(distinct dw_d_id) as reach')
        ).repartition(npar)
    wt4.write.mode('overwrite').parquet(out_table_path)
    print('end', datetime.now())
    if debug:
        res = spark.read.parquet(out_table_path)
        res.show()
        print(res.count())

def sanity_check():
    df=spark.read.parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/inventory_v2/distribution_of_quarter_data/tournament=wc2021/cd=2021-10-27/').toPandas()
    df2=spark.read.parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/inventory_wt/cohort_agg_quarter/tournament=wc2021/cd=2021-10-27/').toPandas()
    print(df2.ad_time.sum()/df.ad_time.sum()) # 97.3%
    print(df2.reach.sum()/df.reach.sum()) # 94.4%

def main():
    tournaments=['wc2022', 'wc2021']
    for tour in tournaments:
        pl = preprocess_playout(spark.read.parquet(playout_path + tour)).toPandas()
        pl['cd'] = pl.break_start.map(lambda x: str(x.date()))
        dates = sorted(set(pl.cd))
        for dt in dates:
            try:
                process(tour, dt, pl[pl.cd == dt])
            except:
                print(dt, 'exception!')


if __name__ == '__main__':
    main()
