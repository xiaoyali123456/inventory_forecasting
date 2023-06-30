import pandas as pd
import sys
from functools import reduce
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from common import *

def load_inventory(cd, n=30):
    last_cd = get_last_cd(INVENTORY_SAMPLING_PATH, cd, n)
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
    map_ = {
        "EMAIL_FEMALE_13-17": "13-17",
        "EMAIL_FEMALE_18-24": "18-24",
        "EMAIL_FEMALE_25-34": "25-34",
        "EMAIL_FEMALE_35-44": "35-44",
        "EMAIL_FEMALE_45-54": "45-54",
        "EMAIL_FEMALE_55-64": "55-64",
        "EMAIL_FEMALE_65PLUS": "65PLUS",
        "EMAIL_MALE_13-17": "13-17",
        "EMAIL_MALE_18-24": "18-24",
        "EMAIL_MALE_25-34": "25-34",
        "EMAIL_MALE_35-44": "35-44",
        "EMAIL_MALE_45-54": "45-54",
        "EMAIL_MALE_55-64": "55-64",
        "EMAIL_MALE_65PLUS": "65PLUS",
        "FB_FEMALE_13-17": "13-17",
        "FB_FEMALE_18-24": "18-24",
        "FB_FEMALE_25-34": "25-34",
        "FB_FEMALE_35-44": "35-44",
        "FB_FEMALE_45-54": "45-54",
        "FB_FEMALE_55-64": "55-64",
        "FB_FEMALE_65PLUS": "65PLUS",
        "FB_MALE_13-17": "13-17",
        "FB_MALE_18-24": "18-24",
        "FB_MALE_25-34": "25-34",
        "FB_MALE_35-44": "35-44",
        "FB_MALE_45-54": "45-54",
        "FB_MALE_55-64": "55-64",
        "FB_MALE_65PLUS": "65PLUS",
        "PHONE_FEMALE_13-17": "13-17",
        "PHONE_FEMALE_18-24": "18-24",
        "PHONE_FEMALE_25-34": "25-34",
        "PHONE_FEMALE_35-44": "35-44",
        "PHONE_FEMALE_45-54": "45-54",
        "PHONE_FEMALE_55-64": "55-64",
        "PHONE_FEMALE_65PLUS": "65PLUS",
        "PHONE_MALE_13-17": "13-17",
        "PHONE_MALE_18-24": "18-24",
        "PHONE_MALE_25-34": "25-34",
        "PHONE_MALE_35-44": "35-44",
        "PHONE_MALE_45-54": "45-54",
        "PHONE_MALE_55-64": "55-64",
        "PHONE_MALE_65PLUS": "65PLUS",
        "FMD009V0051317HIGHSRMLDESTADS": "13-17",
        "FMD009V0051317SRMLDESTADS": "13-17",
        "FMD009V0051824HIGHSRMLDESTADS": "18-24",
        "FMD009V0051824SRMLDESTADS": "18-24",
        "FMD009V0052534HIGHSRMLDESTADS": "25-34",
        "FMD009V0052534SRMLDESTADS": "25-34",
        "FMD009V0053599HIGHSRMLDESTADS": "35-99",
        "FMD009V0053599SRMLDESTADS": "35-99",
        "MMD009V0051317HIGHSRMLDESTADS": "13-17",
        "MMD009V0051317SRMLDESTADS": "13-17",
        "MMD009V0051824HIGHSRMLDESTADS": "18-24",
        "MMD009V0051824SRMLDESTADS": "18-24",
        "MMD009V0052534HIGHSRMLDESTADS": "25-34",
        "MMD009V0052534SRMLDESTADS": "25-34",
        "MMD009V0053599HIGHSRMLDESTADS": "35-99",
        "MMD009V0053599SRMLDESTADS": "35-99",
    }
    if cohort is not None:
        for x in cohort.split('|'):
            if x in map_:
                return map_[x]
    return ''

@F.udf(returnType=StringType())
def device(cohort):
    if cohort is not None:
        dc = {'A_15031263': '15-20K', 'A_94523754': '20-25K', 'A_40990869': '25-35K', 'A_21231588': '35K+'}
        for x in cohort.split('|'):
            if x in dc:
                return x
    return ''


# TODO: add comment for the purpose of this function
def aggregate(df, group_cols, DATE):
    filter = spark.read.parquet(NEW_MATCHES_PATH_TEMPL % DATE) \
        .selectExpr('startdate as cd', 'content_id').distinct()
    return df.join(filter, ['cd', 'content_id']).groupby(
        *group_cols,
        'country', 'language', 'platform', 'city', 'state',
        nccs('cohort').alias('nccs'), 
        device('cohort').alias('device'),
        gender('cohort').alias('gender'),
        age('cohort').alias('age'),
    ).agg(F.sum('ad_time').alias('ad_time'), F.sum('reach').alias('reach')).toPandas()


def moving_avg(df, group_cols, target, alpha=0.2):
    cohort_cols = ['country', 'platform', 'city', 'state', 'nccs', 'device', 'gender', 'age', 'language']
    df2 = df.fillna('')
    df2[target+'_ratio'] = df2[target] / df2.groupby(group_cols)[target].transform('sum')
    df3 = df2.pivot_table(index=group_cols, columns=cohort_cols, values=target+'_ratio', aggfunc='sum').fillna(0)
    # S[n+1] = (1-alpha) * S[n] + alpha * A[n+1]
    df4 = df3.ewm(alpha=alpha, adjust=False).mean().shift(1)
    return df4.iloc[-1].rename(target).reset_index()


def merge_custom_cohort(df, cd, src_col='watch_time', dst_col='ad_time'):
    # df = pd.read_parquet(f'{AD_TIME_SAMPLING_PATH}cd={cd}/')
    ch = pd.read_parquet(f'{CUSTOM_COHORT_PATH}cd={cd}/')
    ch = ch[~(ch.is_cricket==False)]
    ch.segments.fillna('', inplace=True)
    ch2 = (ch.groupby('segments')[src_col].sum().rename(dst_col).rename_axis('custom_cohorts') / ch[src_col].sum()).reset_index()
    df2 = df.merge(ch2, how='cross')
    df2[dst_col] = df2[dst_col+'_x'] * df2[dst_col+'_y']
    return df2.drop(columns=[dst_col+'_x', dst_col+'_y'])


if __name__ == '__main__':
    DATE = sys.argv[1]
    # DATE='2023-05-26'
    iv = load_inventory(DATE)
    giv = aggregate(iv, ['cd', 'content_id'], DATE)
    df = moving_avg(giv, ['cd'], target='ad_time')
    merge_custom_cohort(df, DATE).to_parquet(f'{AD_TIME_SAMPLING_PATH}cd={DATE}/p0.parquet')
    df2 = moving_avg(giv, ['cd'], target='reach')
    merge_custom_cohort(df2, DATE, 'reach', 'reach').to_parquet(f'{REACH_SAMPLING_PATH}cd={DATE}/p0.parquet')
