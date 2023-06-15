import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

NEW_MATCHES_PATH_TEMPL = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/cms_match/cd=%s/'
PREROLL_INVENTORARY_PATH = 's3://hotstar-ads-targeting-us-east-1-prod/trackers/shifu_ad_events/ad_inventory/'
PREROLL_INVENTORARY_AGG_PATH = 's3://hotstar-ads-targeting-us-east-1-prod/trackers/shifu_reporting/aggregates/hourly/ad_inventory/'


def latest_n_day_matches(cd, n):
    matches = spark.read.parquet(NEW_MATCHES_PATH_TEMPL % cd)
    df = matches[['startdate', 'content_id']].distinct().toPandas().sort_values('startdate')
    return df[df.startdate < cd].iloc[-n:]


cd = '2023-06-14'
matches = latest_n_day_matches(cd, 10)

for row in matches.itertuples():
    # process(row.startdate, row.content_id)
    # debug
    cd, content_id = row.startdate, row.content_id
    break

@F.udf(returnType=StringType())
def nccs(segment_list):
    if segment_list is not None:
        for x in segment_list:
            if x.startswith('NCCS_'):
                return x
    return ''

@F.udf(returnType=StringType())
def device(segment_list):
    if segment_list is not None:
        dc = {'A_15031263': '15-20K', 'A_94523754': '20-25K', 'A_40990869': '25-35K', 'A_21231588': '35K+'}
        for x in segment_list:
            if x in dc:
                return dc[x]
    return ''

def process(cd, content_id):
    df = spark.read.parquet(f'{PREROLL_INVENTORARY_AGG_PATH}cd={cd}/')
    df2 = df.where(f"content_id='{content_id}' and lower(ad_placement) = 'preroll'").selectExpr(
        'content_language as language',
        'device_platform as platform',
        'demo_age_range_list[0] as age',
        'demo_gender_list[0] as gender',
        'user_segment_list',
        'country',
        'city',
        'state',
        'inventory'
    )
    df3 = df2.groupby(
        'language', 'platform', 'age', 'gender', 'country', 'city', 'state',
        nccs('user_segment_list').alias('nccs'),
        device('user_segment_list').alias('device'),
    ).agg(F.sum('inventory').alias('inventory'))

def moving_avg(df, group_cols, targets, alpha=0.2):
    cohort_cols = ['country', 'platform', 'city', 'state', 'nccs', 'device', 'gender', 'age'] # + ['language']
    df2 = df.fillna('')
    targets_ratio = [t + '_ratio' for t in targets]
    for tr, t in zip(targets_ratio, targets):
        df2[tr] = df2[t] / df2.groupby(group_cols)[t].transform('sum')
    
    df3 = df2.pivot_table(index=group_cols, columns=cohort_cols, values=target+'_ratio', aggfunc='sum').fillna(0)
    # S[n+1] = (1-alpha) * S[n] + alpha * A[n+1]
    df4 = df3.ewm(alpha=alpha, adjust=False).mean().shift(1)
    return df4.iloc[-1].rename(target).reset_index()
