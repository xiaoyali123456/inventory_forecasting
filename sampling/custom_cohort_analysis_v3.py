import json
import os
from datetime import datetime
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
import s3fs

spark = SparkSession.builder \
    .config("spark.sql.hive.convertMetastoreParquet", "false") \
    .config("hive.metastore.uris", "thrift://metastore.data.hotstar-labs.com:9083") \
    .enableHiveSupport() \
    .getOrCreate()

s3 = s3fs.S3FileSystem()
c_tags = ['AP_2AS', 'AP_107', 'AP_2AT', 'AP_3YF', 'AP_3YC']

t = s3.glob('hotstar-ads-targeting-us-east-1-prod/adw/user-segment/ap_user_tag/cd*/hr*/segment*/')
t2 = s3.glob('hotstar-ads-targeting-us-east-1-prod/adw/user-segment/custom-audience/cd*/hr*/segment*/')

def f(s):
    for c in c_tags:
        if s.endswith(c):
            return True
    return False

t3 = [x for x in t if f(x)] + [x for x in t2 if f(x)]
t4 = ['s3://' + x for x in t3]

# schema of segment
'''
[('dw_d_id', 'string'), ('tag_type', 'string'), ('expiry_time', 'string')]
'''
@F.udf(returnType=StringType())
def concat(tags: set):
    return '|'.join(sorted(tags))

out_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/custom_cohort_inventory_v2/'
WV_S3_BACKUP = 's3://hotstar-ads-ml-us-east-1-prod/data_exploration/data/data_backup/watched_video/'
WV_TABLE = 'data_lake.watched_video'

c_tag_df = spark.read.parquet(*t4)
ct2 = c_tag_df.groupby('dw_d_id').agg(F.collect_set('tag_type').alias('segments'))
ct3 = ct2.withColumn('segments', concat('segments')).cache()

def process(dt, use_backup=False):
    print('process', dt)
    print('begin', datetime.now())
    final_output_path = f'{out_path}cd={dt}/'
    success_path = f'{final_output_path}_SUCCESS'
    if os.system('aws s3 ls ' + success_path) == 0:
        return
    if use_backup:
        wt = spark.read.parquet(f'{WV_S3_BACKUP}cd={dt}/')
    else:
        wt = spark.sql(f'select * from {WV_TABLE} where cd = "{dt}"') \
            .where(F.col('dw_p_id').substr(-1, 1).isin(['2', 'a', 'e', '8']))
    wt1 = wt[['dw_d_id',
        F.expr('lower(genre) == "cricket" as is_cricket'),
        F.expr('lower(language) as language'),
        F.expr('lower(platform) as platform'),
        F.expr('lower(country) as country'),
        F.expr('case when watch_time < 86400 then watch_time else 0 end as watch_time')
    ]]
    npar = 1
    wt2 = wt1.join(ct3, on='dw_d_id', how='left')
    wt3 = wt2.groupby('is_cricket', 'language', 'platform', 'country', 'segments').agg(
        F.expr('sum(watch_time) as watch_time'),
        F.expr('count(distinct dw_d_id) as reach')
    ).repartition(npar)
    wt3.write.parquet(final_output_path)
    print('end', datetime.now())

for dt in pd.date_range('2022-11-08', '2022-11-10'):
    process(str(dt.date()), use_backup=True)

