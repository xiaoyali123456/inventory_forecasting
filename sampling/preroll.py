import os
import pandas as pd
from datetime import datetime
import pyspark.sql.functions as F

NEW_MATCHES_PATH_TEMPL = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/cms_match/cd=%s/'
PREROLL_INVENTORARY_PATH = 's3://hotstar-ads-targeting-us-east-1-prod/trackers/shifu_ad_events/ad_inventory/'
PREROLL_INVENTORARY_AGG_PATH = 's3://hotstar-ads-targeting-us-east-1-prod/trackers/shifu_reporting/aggregates/hourly/ad_inventory/'
PREROLL_DAILY_RATIO_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/preroll/inventory_ratio_daily/'

def latest_n_day_matches(cd, n):
    matches = spark.read.parquet(NEW_MATCHES_PATH_TEMPL % cd)
    df = matches[['startdate', 'content_id']].distinct().toPandas().sort_values('startdate')
    return df[df.startdate < cd].iloc[-n:]


def load_custom_tags(cd: str) -> dict:
    res = {}
    for r in load_requests(cd):
        for x in r.get('customAudiences', []):
            if 'segmentName' in x:
                res[x['segmentName']] = x['customCohort']
    return res


@F.udf(returnType=StringType())
def convert_custom_cohort(long_tags):
    long_tags.sort()
    short_tags = [c_tag_dict[i] for i in long_tags if i in c_tag_dict]  # Need drop duplicates?
    return '|'.join(short_tags)


def process_custom_tags(cd):
    CUSTOM_COHORT_PATH = ''
    global c_tag_dict
    c_tag_dict = load_custom_tags(cd)
    t = s3.glob('hotstar-ads-targeting-us-east-1-prod/adw/user-segment/ap_user_tag/cd*/hr*/segment*/')
    t2 = s3.glob('hotstar-ads-targeting-us-east-1-prod/adw/user-segment/custom-audience/cd*/hr*/segment*/')
    f = lambda x: any(x.endswith(c) for c in c_tag_dict)
    t3 = ['s3://' + x for x in t + t2 if f(x)]
    if len(t3) > 0:
        ct = spark.read.parquet(*t3).groupby('dw_d_id').agg(F.collect_set('tag_type').alias('segments')) \
            .withColumn('segments', convert_custom_cohort('segments'))
        matches_days = latest_match_days(cd, 5) # TODO: filter 3 month expiration
        sql = ','.join(f'"{x}"' for x in matches_days)
        wt = spark.sql(f'select * from {DAU_TABLE} where cd in ({sql})')
        wt1 = wt[['dw_d_id',
            F.expr('lower(cms_genre) == "cricket" as is_cricket'),
            F.expr('case when watch_time < 86400 then watch_time else 0 end as watch_time')
        ]]
        res = wt1.join(ct, on='dw_d_id', how='left').groupby('is_cricket', 'segments').agg(
            F.expr('sum(watch_time) as watch_time'),
            F.expr('count(distinct dw_d_id) as reach')
        )
    else:
        t = pd.DataFrame([[False, '', 1.0, 1.0]], columns=['is_cricket', 'segments', 'watch_time', 'reach'])
        res = spark.createDataFrame(t)
    res.repartition(1).write.mode('overwrite').parquet(f'{CUSTOM_COHORT_PATH}cd={cd}/')



def process(cd, content_id):
    df = spark.read.parquet(f'{PREROLL_INVENTORARY_AGG_PATH}cd={cd}/')
    df2 = df.where(f"content_id='{content_id}' and lower(ad_placement) = 'preroll'").groupby(
        F.expr('demo_gender_list[0] as gender'),
        F.expr('demo_age_range_list[0] as age_bucket'),
        'city',
        'state',
        'location_cluster',
        'pincode',
        F.expr("concat_ws('|', array_sort(ibt_list)) as interest"),
        # 'custom_audience', # extract from recent tables
        'device_brand',
        'device_model',
        # 'primary_sim' # problem
        # 'data_sim' # problem
        F.col('device_platform').alias('platform'),
        F.col('device_os_version').alias('os'),
        F.col('device_app_version').alias('app_version'),
        F.col('user_account_type').alias('subscription_type'),
        'content_type',
        F.lit('cricket').alias('content_genre'),
    ).agg(F.sum('inventory').alias('inventory'))
    df2.coalesce(16).write.parquet(f'{PREROLL_DAILY_RATIO_PATH}cd={cd}/')

cd = '2023-06-14'
matches = latest_n_day_matches(cd, 10)
for row in matches.itertuples():
    print(datetime.now(), row.startdate)
    if os.system(f'aws s3 ls {PREROLL_DAILY_RATIO_PATH}cd={row.startdate}/_SUCCESS') != 0:
        process(row.startdate, row.content_id)
