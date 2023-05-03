import json
import os
from datetime import datetime
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

spark = SparkSession.builder \
    .config("spark.sql.hive.convertMetastoreParquet", "false") \
    .config("hive.metastore.uris", "thrift://metastore.data.hotstar-labs.com:9083") \
    .enableHiveSupport() \
    .getOrCreate()

out_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/custom_cohort_inventory_v1/'
WV_S3_BACKUP = 's3://hotstar-ads-ml-us-east-1-prod/data_exploration/data/data_backup/watched_video/'
WV_TABLE = 'data_lake.watched_video'


@F.udf(returnType=StringType())
def parse(segments):
    if segments is None:
        return None
    try:
        js = json.loads(segments)
    except:
        return None
    if type(js) == list:
        lst = js
    elif type(js) == dict:
        lst =js.get('data', [])
    else:
        return None
    filtered = set()
    equals = ['AP_2AS', 'AP_107', 'AP_2AT', 'AP_3YF', 'AP_3YC'] # custom cohort
    for t in lst:
        match = False
        for s in equals:
            if t == s:
                match = True
                break
        if match:
            filtered.add(t)
    return '|'.join(sorted(filtered))

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
        F.expr('case when watch_time < 86400 then watch_time else 0 end as watch_time'),
        parse('user_segments').alias('segments'),
    ]]
    npar = 8
    wt2 = wt1.groupby('is_cricket', 'language', 'platform', 'country', 'segments').agg(
        F.expr('sum(watch_time) as watch_time'),
        F.expr('count(distinct dw_d_id) as reach')
    ).repartition(npar)
    wt2.write.parquet(final_output_path)
    print('end', datetime.now())

# recent tournament
# for dt in pd.date_range('2023-03-17', '2023-03-22'):
#     process(str(dt.date()))

# wc 2022
for dt in pd.date_range('2022-11-08', '2022-11-10'):
    process(str(dt.date()), use_backup=True)

