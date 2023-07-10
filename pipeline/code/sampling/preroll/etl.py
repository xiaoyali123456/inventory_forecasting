import sys
from datetime import datetime

import pyspark.sql.functions as F

from common import *


def load_new_matches(cd):
    last_cd = get_last_cd(PREROLL_SAMPLING_PATH, cd)
    if last_cd is None:
        last_cd = '2022-10-15' # 10-16 is world cup 22
    matches = spark.read.parquet(MATCH_CMS_PATH_TEMPL % cd) \
        .where(f'startdate > "{last_cd}"').toPandas()
    return matches[matches.sportsseasonname.map(should_be_used_season)]


def process(cd, content_ids):
    print('process', cd)
    print('begin', datetime.now())
    final_output_path = f'{PREROLL_SAMPLING_PATH}cd={cd}/'
    success_path = f'{final_output_path}_SUCCESS'
    if s3.isfile(success_path):
        print('skip')
        return
    preroll = spark.read.parquet(PREROLL_INVENTORY_AGG_PATH + f'cd={cd}/').where(
        F.col('content_id').isin(content_ids) & F.expr("lower(ad_placement) = 'preroll'")
    ).groupby(
        F.expr('demo_gender_list[0] as gender'),
        F.expr('demo_age_range_list[0] as age_bucket'),
        'city',
        'state',
        'location_cluster',
        'pincode',
        F.expr("concat_ws('|', array_sort(ibt_list)) as interest"),
        'device_brand',
        'device_model',
        F.expr('split(device_platform, "")'),
        F.expr('split(device_carrier, "%20")[0] as primary_sim'),
        F.expr('split(device_carrier, "%20")[1] as data_sim'),
        F.col('device_platform').alias('platform'),
        F.col('device_os_version').alias('os'),
        F.col('device_app_version').alias('app_version'),
        F.col('user_account_type').alias('subscription_type'),
        'content_type',
        F.lit('CRICKET').alias('content_genre'),
    ).agg(F.sum('inventory').alias('inventory'))
    preroll.repartition(64).mode('overwrite').write(final_output_path)
    t = preroll.groupby(F.expr('lower(device_carrier)')).sum('inventory').toPandas()

def main(cd):
    matches = load_new_matches(cd)
    for dt in matches.startdate.drop_duplicates():
        content_ids = matches[matches.startdate == dt].content_id.tolist()
        process(dt, content_ids)


if __name__ == '__main__':
    cd = sys.argv[0]
    main(cd)

