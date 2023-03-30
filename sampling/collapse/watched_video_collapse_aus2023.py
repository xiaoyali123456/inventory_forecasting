import json
import pandas as pd
import os

import pyspark.sql.functions as F
from pyspark.sql.types import StringType

spark = SparkSession.builder \
        .config("spark.sql.hive.convertMetastoreParquet", "false") \
        .config("hive.metastore.uris", "thrift://metastore.data.hotstar-labs.com:9083") \
        .enableHiveSupport() \
        .getOrCreate()

out_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/watched_time_for_collapse/'
watched_video_path = 's3://hotstar-dp-datalake-processed-us-east-1-prod/events/watched_video/'

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
    out_table_path = f'{out_path}quarter_data/tournament={tour}/cd={dt}/'
    success_path = f'{out_table_path}_SUCCESS'
    if os.system('aws s3 ls ' + success_path) == 0:
        return
    wt = spark.sql(f'select * from data_lake.watched_video where cd = "{dt}"') \
        .where(F.col('dw_p_id').substr(-1, 1).isin(['2', 'a', 'e', '8']))
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

def batch_process():
    dates = [
        "2023-02-09",
        "2023-02-10",
        "2023-02-11",
        "2023-02-12",
        "2023-02-13",
        "2023-02-17",
        "2023-02-18",
        "2023-02-19",
        "2023-02-20",
        "2023-02-21",
        "2023-03-01",
        "2023-03-02",
        "2023-03-03",
        "2023-03-04",
        "2023-03-05",
        "2023-03-09",
        "2023-03-10",
        "2023-03-11",
        "2023-03-12",
        "2023-03-13",
        "2023-03-17",
        "2023-03-19",
        "2023-03-22"
    ]
    for dt in dates:
        process(dt, 'aus2023')