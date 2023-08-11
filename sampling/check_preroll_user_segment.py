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

df6 = df5.cache()
wv2 = wv[['content_id', 'dw_d_id']].join(df6, on='dw_d_id')

print(df6.count()) # 82406
print(wv.count())  # 24860954
print(wv2.count()) # 482487
print(wv.where('lower(platform) = "androidtv" and content_id = "1540023557"').count()) # 398036
