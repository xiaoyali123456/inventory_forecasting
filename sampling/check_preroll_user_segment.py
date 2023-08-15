import pyspark.sql.functions as F
from pyspark.sql.types import *

PREROLL_INVENTORY_PATH = 's3://hotstar-ads-targeting-us-east-1-prod/trackers/shifu_ad_events/ad_inventory/'
date = '2023-06-11'
df = spark.read.parquet(f'{PREROLL_INVENTORY_PATH}cd={date}/hr=12/')

df2 = df[df.content_id == "1540023557"]
print('total', df2.count()) # 1041910
print('not null', df2[df2.user_segment.isNotNull()].count()) # 1041738

df3 = df2.groupby(['device_platform', 'demo_gender']).count().toPandas()
df3['gender'] = df3.demo_gender.map(lambda s: s.split(',')[0] if isinstance(s, str) else '')

df4 = df3.groupby(['device_platform', 'gender'])['count'].sum()

df5 = df2[df2.device_platform == 'ANDROIDTV']
df5[['demo_gender', 'user_segment']].count()

wv = spark.read.parquet(f's3://hotstar-dp-datalake-processed-us-east-1-prod/events/watched_video/cd={date}/hr=12')
wv2 = wv.selectExpr('content_id', 'dw_d_id', 'user_segments as wv_seg') \
    .join(df5.selectExpr('dw_d_id', 'demo_gender', 'user_segment as pr_seg'), on='dw_d_id')
# wv2.write.parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_v2/check_preroll_sampling_0811/')

print(df6.count()) # 82406
print(wv.count())  # 24860954
print(wv2.count()) # 482487
print(wv.where('lower(platform) = "androidtv" and content_id = "1540023557"').count()) # 398036

# check on new sampling raw data
df = spark.read.parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_v2/inventory/')

@F.udf(returnType=StringType())
def unify_gender(cohort):
    if cohort is not None:
        for x in cohort.split('|'):
            if x.startswith('FMD00') or '_FEMALE_' in x:
                return 'f'
            if x.startswith('MMD00') or '_MALE_' in x:
                return 'm'
    return ''

@F.udf(returnType=StringType())
def get_gender(cohort):
    if cohort is not None:
        for x in cohort.split(','):
            if x.startswith('FMD00') or '_FEMALE_' in x:
                return 'f'
            if x.startswith('MMD00') or '_MALE_' in x:
                return 'm'
    return ''

df2 = df.where('cd = "2023-06-11" and platform == "androidtv"') \
    .withColumn('gender', unify_gender('cohort')) \
    .toPandas()
df2[(df2.cohort == '')|(df2.cohort.isna())]
df2[(df2.cohort == '')|(df2.cohort.isna())].reach.sum()/df2.reach.sum()

df3 = spark.read.parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_v2/inventory_back_up_2023-08-14/')
df4 = df3.where('cd = "2023-06-11" and platform == "androidtv"') \
    .withColumn('gender', unify_gender('cohort')) \
    .toPandas()
df4[(df4.cohort == '')|(df4.cohort.isna())]
df4[(df4.cohort == '')|(df4.cohort.isna())].reach.sum()/df4.reach.sum()

# recheck preroll
df = spark.read.parquet(f'{PREROLL_INVENTORY_PATH}cd={date}/')
df2 = df[df.content_id == "1540023557"]

print('total', df2.count())
print('not null', df2[(df2.user_segment=='')|df2.user_segment.isNull()].count())
# 

df3 = df2.where('lower(device_platform) = "androidtv"')
print('total', df3.count())
print('not null', df3[(df3.user_segment=='')|df3.user_segment.isNull()].count())
# 


df4 = df3.withColumn('gender', get_gender('user_segment'))
df5 = df4.groupby('gender').count().toPandas()
print(df5)
'''
  gender   count
0      f  166192
1          48498
2      m  988861
'''

wv = spark.read.parquet('s3://hotstar-dp-datalake-processed-us-east-1-prod/events/watched_video/cd=2023-06-11/') \
    .where('content_id = "1540023557" and lower(platform) = "androidtv"')
print(wv[['dw_d_id']].count()) # 15679685
print(wv[['dw_d_id']].join(df4[['dw_d_id', 'gender']].distinct(), on='dw_d_id').count()) # 9951695
print(wv[['dw_d_id']].join(df4[['dw_d_id']], on='dw_d_id', how='left_anti').count())

print(wv[['dw_d_id']].distinct().count()) # 862227
print(wv[['dw_d_id']].distinct().join(df4[['dw_d_id']], on='dw_d_id', how='left_anti').count()) # 226141

# double check
print(wv[['dw_d_id']].join(df[['dw_d_id']], on='dw_d_id', how='left_anti').count()) # 5714105
print(wv[['dw_d_id']].distinct().join(df[['dw_d_id']], on='dw_d_id', how='left_anti').count()) # 225251

wv2 = spark.read.parquet('s3://hotstar-dp-datalake-processed-us-east-1-prod/events/watched_video/cd=2023-06-11/') \
    .where('content_id = "1540023557"')
print(wv2[['dw_d_id']].count())
print(wv2[['dw_d_id']].join(df3[['dw_d_id']], on='dw_d_id', how='left_anti').count())
