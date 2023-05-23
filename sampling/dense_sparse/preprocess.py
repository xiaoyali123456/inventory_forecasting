import json
import pandas as pd
import json
import os
import re

import pyspark.sql.functions as F
from pyspark.sql.types import StringType

output_root = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/dense_sparse/qdata/'
watched_video_path = 's3://hotstar-ads-ml-us-east-1-prod/data_exploration/data/data_backup/watched_video/'
WV_TABLE = 'data_lake.watched_video'

spark = SparkSession.builder \
    .config("spark.sql.hive.convertMetastoreParquet", "false") \
    .config("hive.metastore.uris", "thrift://metastore.data.hotstar-labs.com:9083") \
    .enableHiveSupport() \
    .getOrCreate()

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
    prefixs = ['NCCS_', 'FMD00', 'MMD00', 'CITY_', 'STATE_']
    middles = ['_MALE_', '_FEMALE_']
    for t in lst:
        match = False
        if t.startswith('SSAI::'):
            for s in t.split(':'):
                if s.startswith('C'):
                    filtered.add(s)
        if not match:
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


def process(dt, tour='wc2022', use_backup=True):
    print('process', dt)
    print('begin', pd.datetime.now())
    npar = 32
    final_output_path = f'{output_root}/tournament={tour}/cd={dt}/'
    success_path = f'{final_output_path}_SUCCESS'
    if os.system('aws s3 ls ' + success_path) == 0:
        return
    if use_backup:
        wt = spark.read.parquet(f'{watched_video_path}cd={dt}/')
    else:
        wt = spark.sql(f'select * from {WV_TABLE} where cd = "{dt}"') \
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
    basic = ['is_cricket', 'language', 'platform', 'country', 'city', 'state', 'segments']
    wt2 = wt1.groupby(*basic).agg(
            F.expr('sum(watch_time) as watch_time'),
            F.expr('count(distinct dw_d_id) as reach')
        ).repartition(npar)
    wt2.write.mode('overwrite').parquet(final_output_path)
    print('end', pd.datetime.now())


def batch_process():
    dates = pd.read_json('dates.json')[0]
    for dt in dates:
        process(dt)

def batch_process2():
    for dt in [
        '2022-08-27',
        '2022-08-28',
        '2022-08-30',
        '2022-08-31',
        '2022-09-01',
        '2022-09-02',
        '2022-09-03',
        '2022-09-04',
    ]:
        process(dt, tour='other', use_backup=True)


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


prog = re.compile(r"C\d+_[^|]+")
@F.udf(returnType=StringType())
def custom(segments):
    if segments is not None:
        res = prog.findall(segments)
        if len(res):
            return '|'.join(res)
    return 'other'


def postprocess():
    df = spark.read.parquet(output_root)
    df2 = df.groupby(
        'cd', 'is_cricket', 'tournament',
        'country', 'language', 'platform', 'city', 'state',
        nccs('segments').alias('nccs'), 
        device('segments').alias('device'),
        gender('segments').alias('gender'),
        age('segments').alias('age'),
        custom('segments').alias('custom')
    ).agg(F.sum('watch_time').alias('watch_time'),
          F.sum('reach').alias('reach'))
    df2.repartition(8).write.mode('overwrite').parquet(f"{output_root.strip('/')}_v2/")


def reach(dt):
    wt = spark.read.parquet(watched_video_path + 'cd=' + dt)
    return wt.where("lower(genre) == 'cricket'")[['dw_d_id']].distinct().count()


def batch_reach():
    df = pd.read_json('dates.json')
    df.columns = ['cd']
    res = []
    for dt in df.cd:
        print(dt)
        res.append(reach(dt))
        df['reach'] = pd.Series(res)
        df.to_csv('reach.csv', index=False)


if __name__ == '__name__':
    batch_process()
    postprocess()
