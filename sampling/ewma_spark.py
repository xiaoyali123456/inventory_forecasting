import sys
from functools import reduce
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

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
    ).agg(F.sum('ad_time').alias('ad_time'), F.sum('reach').alias('reach'))


# mean error: 0.8%, but max cohort:2.7% vs 3.3%
def moving_avg(df, group_cols, target, lambda_=0.8):
    cohort_cols = ['country', 'language', 'platform', 'city', 'state', 'nccs', 'device', 'gender', 'age']
    df2 = df.groupby(group_cols).agg(F.sum(target).alias('gsum'))
    df3 = df.join(df2, group_cols).withColumn(target, F.col(target)/F.col('gsum'))
    group_cols = [F.col(x).desc() for x in group_cols]
    df4 = df3.withColumn(target, F.col(target) * F.lit(lambda_) * F.pow(
            F.lit(1-lambda_),
            F.row_number().over(
                Window.partitionBy(cohort_cols).orderBy(group_cols)
            )-F.lit(1)
        )
    )
    return df4.groupby(cohort_cols).agg(F.sum(target).alias(target))


if __name__ == '__main__':
    DATE = sys.argv[1]
    iv = load_inventory(DATE)
    piv = augment(iv, ['cd', 'content_id'], DATE)
    df = moving_avg(piv, ['cd'], target='ad_time')
    df.repartition(1).write.parquet(f'{AD_TIME_SAMPLING_PATH}cd={DATE}')
    df2 = moving_avg(piv, ['cd'], target='reach')
    df2.repartition(1).write.parquet(f'{REACH_SAMPLING_PATH}cd={DATE}')
