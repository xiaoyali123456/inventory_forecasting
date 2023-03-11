# To fix date format in watched_video
# PYSPARK_DRIVER_PYTHON_OPTS='-m IPython' pyspark --name inv_forecast

import json
import pandas as pd
import os

import pyspark.sql.functions as F
from pyspark.sql.types import StringType

out_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/watched_time_for_collapse/'
watched_video_path = 's3://hotstar-ads-ml-us-east-1-prod/data_exploration/data/data_backup/watched_video/'

@F.udf(returnType=StringType())
def parse(segments):
    try:
        js = json.loads(segments)
        if type(js) == list:
            lst = js
        else:
            lst =js.get('data', [])
    except:
        return None
    filtered = set()
    equals = ['A_15031263', 'A_94523754', 'A_40990869', 'A_21231588', # device price
              'A_34365936', 'A_49094287', 'AP_107', 'AP_2AS', 'AP_2AT'] # sponsor custom cohort
    prefixs = ['NCCS_', 'FMD00', 'MMD00']
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

def process(dt, tour):
    print('process', dt)
    print('begin', pd.datetime.now())
    npar = 32
    # out_table_path = f'{out_path}quarter_data/tournament=wc2022/cd={dt}/'
    out_table_path = f'{out_path}quarter_data/tournament={tour}/cd={dt}/'
    success_path = f'{out_table_path}_SUCCESS'
    if os.system('aws s3 ls ' + success_path) == 0:
        return
    wt_path = f'{watched_video_path}cd={dt}/'
    wt = spark.read.parquet(wt_path)
    # debug = True
    debug = False
    if debug:
        wt = spark.read.parquet('s3://hotstar-ads-ml-us-east-1-prod/data_exploration/data/data_backup/watched_video/cd=2022-10-16/part-00163-7313c8ab-82bf-482e-be26-aeedfe9e9478-c000.snappy.parquet')
        out_table_path = f'{out_path}debug_out_table/'
        npar = 1
    wt1 = wt[['dw_d_id',
        F.expr('lower(genre) == "cricket" as is_cricket'),
        F.expr('lower(language) as language'),
        F.expr('lower(platform) as platform'),
        F.expr('lower(country) as country'),
        F.expr('lower(city) as city'),
        F.expr('lower(state) as state'),
        F.expr('case when watch_time < 86400 then watch_time else 0 end as watch_time'),
        parse('user_segments').alias('segments'),
    ]]
    wt2 = wt1.groupby('is_cricket', 'language', 'platform', 'country', 'city', 'state', 'segments').agg(
            F.expr('sum(watch_time) as watch_time'),
            F.expr('count(distinct dw_d_id) as reach')
        ).repartition(npar)
    wt2.write.mode('overwrite').parquet(out_table_path)
    print('end', pd.datetime.now())
    if debug:
        res = spark.read.parquet(out_table_path)
        res.show()
        print(res.count())

def batch_process():
    for dt in pd.date_range('2022-10-16', '2022-11-13'):
        process(dt.date(), 'wc2022')
    process('2022-11-15')
    for dt in pd.date_range('2021-10-17', '2021-11-14'):
        process(dt.date(), 'wc2021')

@F.udf(returnType=StringType())
def nccs(segments):
    if segments is not None:
        for x in segments.split('|'):
            if x.startswith('NCCS_'):
                return x
    return 'other'

@F.udf(returnType=StringType())
def gender(segments):
    if segments is not None:
        for x in segments.split('|'):
            if x.startswith('FMD00') or '_FEMALE_' in x:
                return 'f'
            if x.startswith('MMD00') or '_MALE_' in x:
                return 'm'
    return 'other'

@F.udf(returnType=StringType())
def age(segments):
    dc = {
        'FB_MALE_35-44',
        'FB_MALE_45-54',
        'FB_MALE_55-64',
        'FB_MALE_65PLUS',
        'FB_FEMALE_35-44',
        'FB_FEMALE_45-54',
        'FB_FEMALE_55-64',
        'FB_FEMALE_65PLUS',
        'FB_BARC_FEMALE_31-40',
        'FB_BARC_FEMALE_41-50',
        'FB_BARC_FEMALE_51+',
        'FB_BARC_MALE_31-40',
        'FB_BARC_MALE_41-50',
        'FB_BARC_MALE_51+',
        'EMAIL_MALE_35-44',
        'EMAIL_MALE_45-54',
        'EMAIL_MALE_55-64',
        'EMAIL_MALE_65PLUS',
        'EMAIL_FEMALE_35-44',
        'EMAIL_FEMALE_45-54',
        'EMAIL_FEMALE_55-64',
        'EMAIL_FEMALE_65PLUS',
        'EMAIl_BARC_FEMALE_31-40',
        'EMAIl_BARC_FEMALE_41-50',
        'EMAIl_BARC_FEMALE_51+',
        'EMAIl_BARC_MALE_31-40',
        'EMAIl_BARC_MALE_41-50',
        'EMAIl_BARC_MALE_51+',
        'PHONE_MALE_35-44',
        'PHONE_MALE_45-54',
        'PHONE_MALE_55-64',
        'PHONE_MALE_65+',
        'PHONE_FEMALE_35-44',
        'PHONE_FEMALE_45-54',
        'PHONE_FEMALE_55-64',
        'PHONE_FEMALE_65+',
        'PHONE_MALE_TV_31-40',
        'PHONE_MALE_TV_41-50',
        'PHONE_MALE_TV_51-60',
        'PHONE_MALE_TV_60+',
        'PHONE_FEMALE_TV_31-40',
        'PHONE_FEMALE_TV_41-50',
        'PHONE_FEMALE_TV_51-60',
        'PHONE_FEMALE_TV_60+',
        'PHONE_BARC_FEMALE_31-40',
        'PHONE_BARC_FEMALE_41-50',
        'PHONE_BARC_FEMALE_51+',
        'PHONE_BARC_MALE_31-40',
        'PHONE_BARC_MALE_41-50',
        'PHONE_BARC_MALE_51+',
        'FMD009V0053599SRMLDESTADS',
        'MMD009V0053599SRMLDESTADS',
        'FMD009V0053599HIGHSRMLDESTADS',
        'MMD009V0053599HIGHSRMLDESTADS',
    }
    if segments is not None:
        for x in segments.split('|'):
            if x in dc:
                return '30+'
    return 'other'

@F.udf(returnType=StringType())
def device(segments):
    if segments is not None:
        dc = {'A_40990869': '25K+', 'A_21231588': '25K+'}
        for x in segments.split('|'):
            if x in dc:
                return dc[x]
    return 'other'

def postprocess():
    df = spark.read.parquet(f'{out_path}quarter_data/')
    pdf = df.groupby(
        'cd', 'is_cricket',
        'country', 'language', 'platform', 'city', 'state',
        nccs('segments').alias('nccs'), 
        device('segments').alias('device'),
        gender('segments').alias('gender'),
        age('segments').alias('age'),
    ).agg(F.sum('watch_time').alias('watch_time'),
          F.sum('reach').alias('reach'))
    pdf.repartition(8).write.mode('overwrite').parquet(f'{out_path}quarter_data_v2')

if __name__ == '__main__':
    batch_process()
    postprocess()