import sys
from datetime import datetime, timedelta

import pyspark.sql.functions as F
from pyspark.sql.types import StringType

from common import *

def should_be_used_season(sport_season_name):
    if isinstance(sport_season_name, str):
        sport_season_name = sport_season_name.lower()
        tournaments = [
            'icc world test championship',
            "icc men's cwc qualifier",
        ] + FOCAL_TOURNAMENTS
        for t in tournaments:
            if t in sport_season_name:
                return True
    return False


def load_new_matches(cd):
    last_cd = get_last_cd(PREROLL_SAMPLING_PATH, cd)
    if last_cd is None:
        last_cd = '2022-10-15' # 10-16 is world cup 22
    matches = spark.read.parquet(MATCH_CMS_PATH_TEMPL % cd) \
        .where(f'startdate > "{last_cd}"').toPandas()
    last_available_date = (datetime.today()-timedelta(90)).strftime('%Y-%m-%d') # S3 glacier deadline
    matches = matches[matches.startdate >= last_available_date]
    return matches[matches.sportsseasonname.map(should_be_used_season)]


@F.udf(returnType=StringType())
def parse_carrier(carrier):
    allow_list = ['bsnl', 'vodafone', 'vi', 'idea', 'airtel', 'jio'] # 'others'
    for x in allow_list:
        if x in carrier.lower():
            return x
    return 'other'


def process(cd, content_ids):
    print('process', cd)
    print('begin', datetime.now())
    final_output_path = f'{PREROLL_SAMPLING_PATH}cd={cd}/'
    success_path = f'{final_output_path}_SUCCESS'
    if s3.isfile(success_path):
        print('skip')
        return
    preroll = spark.read.parquet(PREROLL_INVENTORY_PATH + f'cd={cd}/').where(
        F.col('content_id').isin(content_ids) & F.expr("lower(ad_placement) = 'preroll'")
    ).groupby(
        F.expr('split(demo_gender, ",")[0] as gender'),
        F.expr('split(demo_age_range, ",")[0] as age_bucket'),
        'city',
        'state',
        'location_cluster',
        'pincode',
        F.expr("ibt as interest"), # separated by comma
        'device_brand',
        'device_model',
        parse_carrier('device_carrier').alias('primary_sim'),
        F.expr('lower(device_network_data) as data_sim'),
        F.col('device_platform').alias('platform'),
        F.col('device_os_version').alias('os'),
        F.col('device_app_version').alias('app_version'),
        F.col('user_account_type').alias('subscription_type'),
        'content_type',
        F.lit('cricket').alias('content_genre'),
    ).agg(F.count('dw_d_id').alias('inventory'), F.expr('count(distinct dw_d_id) as reach'))
    preroll.repartition(64).write.mode('overwrite').parquet(final_output_path)
    print('end', datetime.now())


def main(cd):
    matches = load_new_matches(cd)
    for dt in matches.startdate.drop_duplicates():
        content_ids = matches[matches.startdate == dt].content_id.tolist()
        process(dt, content_ids)


if __name__ == '__main__':
    cd = sys.argv[0]
    main(cd)

