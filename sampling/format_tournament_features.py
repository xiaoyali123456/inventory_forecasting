import pandas as pd

input_file = "data/all_features_v5.csv"
output_file = "data/holidays_v5.csv"
# df0 = pd.read_clipboard()
# df0 = pd.read_csv('data/all_features_v3_2.csv')
df0 = pd.read_csv(input_file)

df = df0.rename(columns=lambda x: x.strip())
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
df['ds'] = pd.to_datetime(df.date).map(lambda x: str(x.date()))
df = df[df.abandoned != 1].copy()

def date_range_to_holidays(dates, holiday: str):
    return pd.DataFrame({
        'ds': dates.reset_index(drop=True).map(lambda x: str(x.date())),
        'holiday': holiday,
    })

lockdown = pd.concat([
    pd.date_range('2020-03-24', '2020-05-31').to_series(),
    # pd.date_range('2020-03-24', '2020-04-14').to_series(),
    # pd.date_range('2020-04-15', '2020-05-03').to_series(),
    # pd.date_range('2020-05-04', '2020-05-17').to_series(),
    # pd.date_range('2020-05-18', '2020-05-31').to_series(),
    # pd.date_range('2021-04-05', '2021-06-15').to_series(),
])
lockdown = date_range_to_holidays(lockdown, 'lockwdown')
svod = date_range_to_holidays(pd.date_range('2020-03-29', '2023-08-23').to_series(), 'svod_dates')
# svod = date_range_to_holidays(pd.date_range('2020-09-19', '2023-08-23').to_series(), 'svod_dates')

def day_group(df, col):
    return df.groupby('ds')[col].max().rename('holiday').reset_index()

df['if_contain_india_team'].replace({1: 'india_team', 0: 'no_india_team'}, inplace=True)
df['super_match'] = df['if_contain_india_team'] + '_' + df['tournament_type'] + '_' + df['vod_type']

df2 = pd.concat([
    lockdown,
    svod,
    day_group(df, 'match_stage').dropna(),
    day_group(df, 'match_type'),
    day_group(df[df['if_contain_india_team'] == 'india_team'], 'if_contain_india_team'),
    day_group(df, 'tournament_type'),
    day_group(df, 'tournament_name'),
    day_group(df, 'vod_type'),
    day_group(df[df['if_contain_india_team'] == 'india_team'], 'super_match'),
])

df2['lower_window'] = 0
df2['upper_window'] = 0
df2.to_csv(output_file, index=False)
print(len(df2))


df0 = pd.read_csv('data/holidays_v4.csv')
df1 = pd.read_csv('data/holidays_v5.csv')
print(len(df0))
print(len(df1))

print(set(df1['ds'])-set(df0['ds']))
print(set(df1['ds'])-set(df0['ds']))

# for col in ['platform', 'ageBucket', 'city', 'state', 'devicePrice', 'gender', 'language']:
#     print(col)
#     res = df.where('matchId = "708501"').groupby(col).agg(F.count('reach'), F.sum('reach'), F.sum('inventory'))
#     res.show(100, False)
#     res.toPandas().to_csv(col+'.csv')

# git clone git@github.com:hotstar/live-ads-inventory-forecasting-ml.git
# pip install pandas==1.3.5 pyarrow==12.0.1 s3fs==2023.1.0 prophet
df.where('lower(platform)="androidtv"').where((F.col('user_segments').contains('MMD00')) | (F.col('user_segments').contains('_MALE_')))\
    .select('dw_d_id', 'content_id', 'country', 'language', 'platform', 'city', 'state', 'user_segments').show(1000, False)
df.where('lower(platform)="androidtv"').where((F.col('user_segments').contains('FMD00')) | (F.col('user_segments').contains('_FEMALE_')))\
    .select('dw_d_id', 'content_id', 'country', 'language', 'platform', 'city', 'state', 'user_segments').show(1000, False)
df.where('lower(platform)="firetv"').where((F.col('user_segments').contains('FMD00')) | (F.col('user_segments').contains('_FEMALE_')))\
    .select('dw_d_id', 'content_id', 'country', 'language', 'platform', 'city', 'state', 'user_segments').show(10, False)
df.where('lower(platform)="androidtv" and user_segments is not null').select('dw_d_id', 'content_id', 'country', 'language', 'platform', 'city', 'state', 'user_segments').show(10, False)
df.where('lower(platform)="androidtv" and user_segments is not null').show()

raw_wt = spark.sql(f'select * from {WV_TABLE} where cd = "2023-03-22"') \
        .where(F.col('dw_p_id').substr(-1, 1).isin(['2', 'a', 'e', '8']))
spark.sql(f'select * from {WV_TABLE} where cd = "2023-03-22"').where('lower(platform)="androidtv" and user_segments is not null').show()


preroll = spark.read.parquet(f'{PREROLL_INVENTORY_PATH}cd={date}') \
        .select('dw_d_id', 'user_segment', 'content_id', 'device_platform').dropDuplicates(['dw_d_id']) \
        .select('dw_d_id', 'user_segment', 'content_id', 'device_platform', make_segment_str_wrapper('user_segment').alias('preroll_cohort')).cache()
preroll.where('lower(device_platform)="androidtv" and user_segment is not null').where((F.col('user_segment').contains('FMD00')) | (F.col('user_segment').contains('_FEMALE_'))).show(10, False)

