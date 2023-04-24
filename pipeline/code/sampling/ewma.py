import pandas as pd
import numpy as np
import sys
from functools import reduce
from common import *

def load_inventory(cd):
    last_cd = get_last_cd(INVENTORY_SAMPLING_PATH, cd, n=30)
    lst = [spark.read.parquet(f'{INVENTORY_SAMPLING_PATH}cd={i}').withColumn('cd', F.lit(i)) for i in last_cd]
    return reduce(lambda x,y: x.union(y), lst)

@F.udf(returnType=StringType())
def nccs(cohort):
    if cohort is not None:
        for x in cohort.split('|'):
            if x.startswith('NCCS_'):
                return x
    return ''

@F.udf(returnType=StringType())
def gender(cohort):
    if cohort is not None:
        for x in cohort.split('|'):
            if x.startswith('FMD00') or '_FEMALE_' in x:
                return 'f'
            if x.startswith('MMD00') or '_MALE_' in x:
                return 'm'
    return ''

@F.udf(returnType=StringType())
def age(cohort):
    u30 = {
        'FB_FEMALE_13-17',
        'FB_FEMALE_18-24',
        'FB_FEMALE_25-34',
        'FB_FEMALE_TV_2-14',
        'FB_FEMALE_TV_15-21',
        'FB_FEMALE_TV_22-30',
        'FB_BARC_FEMALE_15-21',
        'FB_BARC_FEMALE_22-30',
        'FB_MALE_13-17',
        'FB_MALE_18-24',
        'FB_MALE_25-34',
        'FB_MALE_TV_2-14',
        'FB_MALE_TV_15-21',
        'FB_MALE_TV_22-30',
        'FB_BARC_MALE_15-21',
        'FB_BARC_MALE_22-30',
        'EMAIL_FEMALE_13-17',
        'EMAIL_FEMALE_18-24',
        'EMAIL_FEMALE_25-34',
        'EMAIL_BARC_FEMALE_15-21',
        'EMAIL_BARC_FEMALE_22-30',
        'EMAIL_MALE_13-17',
        'EMAIL_MALE_18-24',
        'EMAIL_MALE_25-34',
        'EMAIL_BARC_MALE_15-21',
        'EMAIL_BARC_MALE_22-30',
        'PHONE_FEMALE_13-17',
        'PHONE_FEMALE_18-24',
        'PHONE_FEMALE_25-34',
        'PHONE_FEMALE_TV_2-14',
        'PHONE_FEMALE_TV_15-21',
        'PHONE_FEMALE_TV_22-30',
        'PHONE_BARC_FEMALE_15-21',
        'PHONE_BARC_FEMALE_22-30',
        'PHONE_MALE_13-17',
        'PHONE_MALE_18-24',
        'PHONE_MALE_25-34',
        'PHONE_BARC_MALE_15-21',
        'PHONE_BARC_MALE_22-30',
        'PHONE_MALE_TV_2-14',
        'PHONE_MALE_TV_15-21',
        'PHONE_MALE_TV_22-30',
        'FMD009V0051317SRMLDESTADS',
        'FMD009V0051317HIGHSRMLDESTADS',
        'FMD009V0051334HIGHSRMLDESTADS',
        'FMD009V0051334SRMLDESTADS',
        'FMD009V0051824HIGHSRMLDESTADS',
        'FMD009V0051824SRMLDESTADS',
        'FMD009V0052534HIGHSRMLDESTADS',
        'FMD009V0052534SRMLDESTADS',
        'MMD009V0051317HIGHSRMLDESTADS',
        'MMD009V0051317SRMLDESTADS',
        'MMD009V0051334HIGHSRMLDESTADS',
        'MMD009V0051334SRMLDESTADS',
        'MMD009V0051824HIGHSRMLDESTADS',
        'MMD009V0051824SRMLDESTADS',
        'MMD009V0052534HIGHSRMLDESTADS',
        'MMD009V0052534SRMLDESTADS',
    }
    if cohort is not None:
        for x in cohort.split('|'):
            if x in u30:
                return 'U30'
    return ''

@F.udf(returnType=StringType())
def device(cohort):
    if cohort is not None:
        dc = {'A_15031263': '15-20K', 'A_94523754': '20-25K', 'A_40990869': '25-35K', 'A_21231588': '35K+'}
        for x in cohort.split('|'):
            if x in dc:
                return dc[x]
    return ''


def augment(df, group_cols, DATE):
    filter = spark.read.parquet(NEW_MATCHES_PATH_TEMPL % DATE) \
        .selectExpr('startdate as cd', 'content_id')
    return df.join(filter, ['cd', 'content_id']).groupby(
        *group_cols,
        'country', 'language', 'platform', 'city', 'state',
        nccs('cohort').alias('nccs'), 
        device('cohort').alias('device'),
        gender('cohort').alias('gender'),
        age('cohort').alias('age'),
    ).agg(F.sum('ad_time').alias('ad_time'), F.sum('reach').alias('reach')).toPandas()


def moving_avg(df, group_cols, target, lambda_=0.8):
    cohort_cols = ['country', 'language', 'platform', 'city', 'state', 'nccs', 'device', 'gender', 'age']
    df2 = df.fillna('')
    df2[target+'_ratio'] = df2[target] / df2.groupby(group_cols)[target].transform('sum')
    df3 = df2.pivot(group_cols, cohort_cols, target+'_ratio').fillna(0)
    fun = np.frompyfunc(lambda x,y: lambda_ * x + (1-lambda_) * y, 2, 1) # x is the sum
    df4 = pd.concat([fun.accumulate(df3[x], dtype=object) for x in df3.columns], axis=1).shift(1)
    df4.columns.names = cohort_cols
    return df4.iloc[-1].rename(target)[len(group_cols):].reset_index()

if __name__ == '__main__':
    DATE = sys.argv[1]
    iv = load_inventory(DATE)
    piv = augment(iv, ['cd', 'content_id'], DATE)
    df = moving_avg(piv, ['cd'], target='ad_time')
    df.to_paquert(f'{AD_TIME_SAMPLING_PATH}cd={DATE}/p0.parquet')
    df2 = moving_avg(piv, ['cd'], target='reach')
    df2.to_parquet(f'{REACH_SAMPLING_PATH}cd={DATE}/p0.parquet')
