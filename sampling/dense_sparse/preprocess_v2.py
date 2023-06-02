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
      "PHONE_MALE_25-34": "18T30",
      "PHONE_MALE_18-24": "18T30",
      "FMD009V0051334SRMLDESTADS": "",
      "FMD009V0051334HIGHSRMLDESTADS": "",
      "MMD009V0051334SRMLDESTADS": "",
      "PHONE_FEMALE_25-34": "18T30",
      "PHONE_FEMALE_18-24": "18T30",
      "FMD009V0051824SRMLDESTADS": "18T30",
      "MMD009V0051334HIGHSRMLDESTADS": "",
      "PHONE_FEMALE_35-44": "30P",
      "MMD009V0052534SRMLDESTADS": "18T30",
      "FMD009V0052534SRMLDESTADS": "18T30",
      "FMD009V0053599SRMLDESTADS": "30P",
      "MMD009V0051824SRMLDESTADS": "18T30",
      "PHONE_MALE_45-54": "30P",
      "MMD009V0053599SRMLDESTADS": "30P",
      "PHONE_FEMALE_45-54": "30P",
      "PHONE_MALE_65PLUS": "",
      "PHONE_MALE_13-17": "",
      "PHONE_MALE_55-64": "30P",
      "PHONE_MALE_65+" : "30P",
      "PHONE_FEMALE_55-64": "30P",
      "PHONE_FEMALE_65+" : "30P",
      "MMD009V0051824HIGHSRMLDESTADS": "18T30",
      "FMD009V0051824HIGHSRMLDESTADS": "18T30",
      "PHONE_FEMALE_65PLUS": "",
      "PHONE_FEMALE_13-17": "",
      "FB_MALE_25-34": "18T30",
      "MMD009V0052534HIGHSRMLDESTADS": "18T30",
      "FB_FEMALE_25-34": "18T30",
      "FB_MALE_18-24": "18T30",
      "FB_MALE_35-44": "30P",
      "FMD009V0053599HIGHSRMLDESTADS": "30P",
      "FMD009V0052534HIGHSRMLDESTADS": "18T30",
      "MMD009V0053599HIGHSRMLDESTADS": "30P",
      "FB_FEMALE_18-24": "18T30",
      "FB_FEMALE_35-44": "30P",
      "FB_MALE_45-54": "30P",
      "FB_FEMALE_45-54": "30P",
      "FMD009V0051317SRMLDESTADS": "",
      "MMD009V0051317SRMLDESTADS": "",
      "FB_MALE_55-64": "30P",
      "FB_FEMALE_55-64": "30P",
      "FB_MALE_65PLUS": "30P",
      "FB_MALE_13-17": "",
      "EMAIL_FEMALE_25-34": "18T30",
      "FB_FEMALE_65PLUS": "30P",
      "EMAIL_MALE_25-34": "18T30",
      "EMAIL_FEMALE_18-24": "18T30",
      "MMD009V0051317HIGHSRMLDESTADS": "",
      "EMAIL_FEMALE_35-44": "30P",
      "EMAIL_MALE_35-44": "30P",
      "EMAIL_MALE_18-24": "18T30",
      "FB_FEMALE_13-17": "",
      "FMD009V0051317HIGHSRMLDESTADS": "",
      "EMAIL_FEMALE_45-54": "30P",
      "EMAIL_MALE_45-54": "30P",
      "EMAIL_FEMALE_55-64": "30P",
      "EMAIL_MALE_55-64": "30P",
      "EMAIL_FEMALE_65PLUS": "30P",
      "EMAIL_MALE_65PLUS": "30P",
      "EMAIL_MALE_13-17": "",
      "EMAIL_FEMALE_13-17": "",
      "PHONE_MALE_35-44" : "30P",
      "R_F1824" : "18T30",
      "R_F2534" : "18T30",
      "R_M1824" : "18T30",
      "R_M2534" : "18T30",
      "R_F3599" : "30P",
      "R_M3599" : "30P",
      "18T30" : "18T30",
      "30P" : "30P"
    }
    if segments is not None:
        for x in segments.split('|'):
            if x in dc:
                return dc[x]
    return 'other'


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
    df = spark.read.parquet(output_root+'tournament=wc2022/')
    df2 = df.groupby(
        'cd', 'is_cricket',
        'country', 'language', 'platform', 'city', 'state',
        nccs('segments').alias('nccs'), 
        device('segments').alias('device'),
        gender('segments').alias('gender'),
        age('segments').alias('age'),
        custom('segments').alias('custom')
    ).agg(F.sum('watch_time').alias('watch_time'),
          F.sum('reach').alias('reach'))
    df2.repartition(8).write.mode('overwrite').parquet(f"{output_root.strip('/')}_v4/")

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
