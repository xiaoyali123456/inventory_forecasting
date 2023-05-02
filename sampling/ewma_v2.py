import pandas as pd
import numpy as np
from tqdm import tqdm
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

def load_inventory():
    path = [
        's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/inventory_v2/distribution_of_quarter_data/tournament=wc2022',
    ]
    df = spark.read.parquet(*path)
    return df

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
        dc = {'A_15031263':'15-20K', 'A_94523754': '20-25K', 'A_40990869': '25-35K', 'A_21231588': '35K+'}
        for x in cohort.split('|'):
            if x in dc:
                return dc[x]
    return ''


def augment(df, time_cols):
    filter = spark.read.parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecastingmatch_data/wc2022/') \
        .selectExpr('date as cd', 'content_id')
    return df.join(filter, ['cd', 'content_id']).groupby(
        *time_cols,
        'country', 'language', 'platform', 'city', 'state',
        nccs('cohort').alias('nccs'), 
        device('cohort').alias('device'),
        gender('cohort').alias('gender'),
        age('cohort').alias('age'),
    ).agg(F.sum('ad_time').alias('ad_time'), F.sum('reach').alias('reach')).toPandas()


def moving_avg(df, time_cols, lambda_=0.8):
    debug=True
    if debug:
        df = piv
        lambda_=0.8
        time_cols=['cd', 'content_id']
        piv.to_parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/tmp/piv.parquet')

    cohort_cols = ['country', 'language', 'platform', 'city', 'state', 'nccs', 'device', 'gender', 'age']
    # df2 = df.fillna('').groupby(time_cols+cohort_cols).sum().reset_index() # XXX: critical for groupby None
    df2 = df.fillna('')
    df2['ad_time_ratio'] = df2.ad_time / df2.groupby(time_cols).ad_time.transform('sum')
    if debug:
        df2.to_parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/tmp/df2.parquet')
    df3 = df2.pivot(time_cols, cohort_cols, 'ad_time_ratio').fillna(0)
    fun = np.frompyfunc(lambda x,y: lambda_ * x + (1-lambda_) * y, 2, 1) # x is the sum
    df4 = pd.concat([fun.accumulate(df3[x], dtype=object) for x in tqdm(df3.columns)], axis=1).shift(1)
    df4.columns.names = cohort_cols
    tail = lambda x: x[10:].sum().sum()
    invy = df2.groupby(time_cols).ad_time.sum()
    for attention in cohort_cols:
        gt = df3.groupby(level=attention, axis=1).sum()
        pr = df4.groupby(level=attention, axis=1).sum()
        err = pr - gt
        # we may need no fillna since pandas.sum defaultly dropna
        invy_err = err.mul(invy, axis=0)
        print(attention,
            'inventory', tail(invy),
            'err', tail(invy_err.abs())/tail(invy),
            'sign_err', tail(invy_err))
    if True:
        df5 = df2.pivot(time_cols, cohort_cols, 'ad_time')
        raw_err = (df4 - df3) * df5
        print('raw_err', tail(raw_err.abs()) / tail(invy))
        for attention in cohort_cols:
            gt = df3.groupby(level=attention, axis=1).sum()
            pr = df4.groupby(level=attention, axis=1).sum()
            top_cols = gt.sum().nlargest(10).index
            t = pd.concat([gt[top_cols], pr[top_cols]], axis=1)
            new_cols = t.columns.tolist()
            for i in range(len(new_cols) // 2, len(new_cols)):
                new_cols[i] += '_Pr'
            t.columns = new_cols
            print(t.reset_index())
    return pd.concat([gt, pr], axis=1)

if __name__ == '__main__':
    iv = load_inventory()
    piv = augment(iv, ['cd', 'content_id'])
    df = moving_avg(piv)
    df.to_csv('ewma.csv')
