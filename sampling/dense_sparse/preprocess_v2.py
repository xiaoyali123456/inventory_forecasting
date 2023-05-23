import re

import pyspark.sql.functions as F
from pyspark.sql.types import StringType

# has SSAI (other sampling result may not have)
output_root = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/dense_sparse/qdata/'

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
    return 'U30'


@F.udf(returnType=StringType())
def device(segments):
    if segments is not None:
        dc = {'A_15031263', 'A_94523754', 'A_40990869', 'A_21231588'}
        for x in segments.split('|'):
            if x in dc:
                return x
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
    df2.repartition(8).write.mode('overwrite').parquet(f"{output_root.strip('/')}_v3/")

def postprocess2():
    # check new matches
    input_lst = ['s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/dense_sparse/qdata/tournament=other/']
    df = spark.read.parquet(*input_lst).where('cd < "2023-01-01"')
    df2 = df.groupby('cd', 'is_cricket',
        'country', 'language', 'platform', 'city', 'state',
        nccs('segments').alias('nccs'), 
        device('segments').alias('device'),
        gender('segments').alias('gender'),
        age('segments').alias('age'),
        custom('segments').alias('custom')
    ).agg(F.sum('watch_time').alias('watch_time'),
          F.sum('reach').alias('reach'))
    df2.repartition(8).write.mode('overwrite').parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/dense_sparse/v6/')
