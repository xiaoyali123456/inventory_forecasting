import os
import pandas as pd
from datetime import datetime
import pyspark.sql.functions as F

NEW_MATCHES_PATH_TEMPL = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/cms_match/cd=%s/'
PREROLL_INVENTORARY_PATH = 's3://hotstar-ads-targeting-us-east-1-prod/trackers/shifu_ad_events/ad_inventory/'
PREROLL_INVENTORARY_AGG_PATH = 's3://hotstar-ads-targeting-us-east-1-prod/trackers/shifu_reporting/aggregates/hourly/ad_inventory/'
PREROLL_DAILY_RATIO_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/preroll/ratio_daily/'

def latest_n_day_matches(cd, n):
    matches = spark.read.parquet(NEW_MATCHES_PATH_TEMPL % cd)
    df = matches[['startdate', 'content_id']].distinct().toPandas().sort_values('startdate')
    return df[df.startdate < cd].iloc[-n:]

def process(cd, content_id):
    df = spark.read.parquet(f'{PREROLL_INVENTORARY_AGG_PATH}cd={cd}/')
    df2 = df.where(f"content_id='{content_id}' and lower(ad_placement) = 'preroll'").groupby(
        F.expr('demo_gender_list[0] as gender'),
        F.expr('demo_age_range_list[0] as age_bucket'),
        'city',
        'state',
        'country',
        'location_cluster',
        'pincode',
        F.expr("concat_ws('|', array_sort(ibt_list)) as interest"),
        # 'custom_audience'
        'device_brand',
        'device_model',
        # 'primary_sim' # problem
        # 'data_sim' # problem
        F.col('device_platform').alias('platform'),
        # 'os',
        F.col('device_app_version').alias('app_version'),
        F.col('device_os_version').alias('os_version'),
        F.col('user_account_type').alias('subsription_type'),
        # 'Subcription Type'
        # 'Subcription Tags'
        'content_type',
        # 'content_genre'
    ).agg(F.sum('inventory').alias('inventory'))
    df2.coalesce(16).write.parquet(f'{PREROLL_DAILY_RATIO_PATH}cd={cd}/')

cd = '2023-06-14'
matches = latest_n_day_matches(cd, 10)
for row in matches.itertuples():
    print(datetime.now(), row.startdate)
    if os.system(f'aws s3 ls {PREROLL_DAILY_RATIO_PATH}cd={row.startdate}/_SUCCESS') != 0:
        process(row.startdate, row.content_id)
    # # debug
    # cd, content_id = row.startdate, row.content_id
    # group_cols = ['cd']
    # break
