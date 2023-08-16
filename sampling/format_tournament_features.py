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
# df.where('lower(platform)="androidtv"').where((F.col('user_segments').contains('MMD00')) | (F.col('user_segments').contains('_MALE_')))\
#     .select('dw_d_id', 'content_id', 'country', 'language', 'platform', 'city', 'state', 'user_segments').show(1000, False)
# df.where('lower(platform)="androidtv"').where((F.col('user_segments').contains('FMD00')) | (F.col('user_segments').contains('_FEMALE_')))\
#     .select('dw_d_id', 'content_id', 'country', 'language', 'platform', 'city', 'state', 'user_segments').show(1000, False)
# df.where('lower(platform)="firetv"').where((F.col('user_segments').contains('FMD00')) | (F.col('user_segments').contains('_FEMALE_')))\
#     .select('dw_d_id', 'content_id', 'country', 'language', 'platform', 'city', 'state', 'user_segments').show(10, False)
# df.where('lower(platform)="androidtv" and user_segments is not null').select('dw_d_id', 'content_id', 'country', 'language', 'platform', 'city', 'state', 'user_segments').show(10, False)
# df.where('lower(platform)="androidtv" and user_segments is not null').show()
# df.where('lower(platform)="androidtv"').groupBy('gender', 'ageBucket').count().show()
# df.where('lower(platform)="firetv"').groupBy('platform', 'gender', 'ageBucket').count().show()
# raw_wt = spark.sql(f'select * from {WV_TABLE} where cd = "2023-03-22"') \
#         .where(F.col('dw_p_id').substr(-1, 1).isin(['2', 'a', 'e', '8']))
# spark.sql(f'select * from {WV_TABLE} where cd = "2023-03-22"').where('lower(platform)="androidtv" and user_segments is not null').show()
#
#
# preroll = spark.read.parquet(f'{PREROLL_INVENTORY_PATH}cd={date}') \
#         .select('dw_d_id', 'user_segment', 'content_id', 'device_platform').dropDuplicates(['dw_d_id']) \
#         .select('dw_d_id', 'user_segment', 'content_id', 'device_platform', make_segment_str_wrapper('user_segment').alias('preroll_cohort')).cache()
# preroll.where('lower(device_platform)="androidtv" and user_segment is not null').where((F.col('user_segment').contains('FMD00')) | (F.col('user_segment').contains('_FEMALE_'))).show(10, False)
from util import *
from path import *
from config import *

df = load_data_frame(spark, "")
a = df.collect()
set1 = {}
set2 = {}
for row in a:
    date1 = row[0]
    title1 = " vs ".join(sorted(row[1].strip().lower().split(" vs ")))
    set1[title1] = date1
    date2 = row[2]
    title2 = " vs ".join(sorted([row[3].strip().lower().replace("netherlands", "west indies"), row[4].strip().lower().replace("netherlands", "west indies")]))
    set2[title2] = date2

df2 = load_hive_table(spark, "adtech.daily_predicted_vv_report").where('cd="2023-08-11"').cache()
l = ["2023-09-05","2023-09-02","2023-09-03","2023-09-04","2023-08-31","2023-08-30","2023-09-06","2023-09-09","2023-09-10","2023-09-12","2023-09-14","2023-09-15","2023-09-17"]
for row in a:
    title = " vs ".join(sorted(row[1].strip().lower().split(" vs ")))
    if title in set2:
        l.append(set2[title])
        print(set2[title])
    else:
        print("error")


for d in l:
    print(df2.where(f'ds="{d}"').select('free_vv').collect()[0][0])

print("")
for d in l:
    print(df2.where(f'ds="{d}"').select('sub_vv').collect()[0][0])

import pyspark.sql.functions as F

df = spark.read.parquet(f'{FINAL_ALL_PREDICTION_PATH}cd=2023-08-15/').cache()
for col in ['platform', 'ageBucket', 'city', 'state', 'devicePrice', 'gender', 'language']:
    print(col)
    df.where('matchId = "708501"').groupby(col).agg(F.count('reach'), F.sum('reach'), F.sum('inventory')).toPandas().to_csv(col+'.csv')


for col in ['platform', 'ageBucket', 'city', 'state', 'devicePrice', 'gender', 'language']:
    print(col)
    # df\
    #     .where('matchId = "708501"')\
    #     .groupby(col).agg(F.count('reach'), F.sum('reach'), F.sum('inventory'), F.sum('inventory')/F.count('reach'))\
    #     .show(20, False)


df2.groupby('platform').count().show(20)

df = df2.toPandas()
target = 'ad_time'
group_cols = ['cd']
cohort_cols = ['country', 'platform', 'city', 'state', 'nccs', 'device', 'gender', 'age', 'language']
# calculate the inventory/reach percentage for each cohort combination
df[target+'_ratio'] = df[target] / df.groupby(group_cols)[target].transform('sum')  # index=cd, cols=country, platform,..., target, target_ratio
# convert each cohort combination to one single column
df.groupby('plaform').count()
target_value_distribution_df = df.pivot_table(index=group_cols, columns=cohort_cols, values=target+'_ratio', aggfunc='sum').fillna(0)  # index=cd, cols=cohort_candidate_combination1, cohort_candidate_combination2, ...
# S[n+1] = (1-alpha) * S[n] + alpha * A[n+1]
res_df = target_value_distribution_df.ewm(alpha=alpha, adjust=False).mean().shift(1)
# return the last row as the prediction results
res_df.iloc[-1].rename(target).reset_index()  # cols=country, platform,..., target

import sys
from functools import reduce

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import *

from util import *
from path import *

inventory_distribution = {}
reach_distribution = {}


@F.udf(returnType=StringType())
def unify_nccs(cohort):
    if cohort is not None:
        for x in cohort.split('|'):
            if x.startswith('NCCS_'):
                return x
    return ''


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
def unify_age(cohort):
    map_ = {
        "EMAIL_FEMALE_13-17": "13-17",
        "EMAIL_FEMALE_18-24": "18-24",
        "EMAIL_FEMALE_25-34": "25-34",
        "EMAIL_FEMALE_35-44": "35-44",
        "EMAIL_FEMALE_45-54": "45-54",
        "EMAIL_FEMALE_55-64": "55-64",
        "EMAIL_FEMALE_65PLUS": "65PLUS",
        "EMAIL_MALE_13-17": "13-17",
        "EMAIL_MALE_18-24": "18-24",
        "EMAIL_MALE_25-34": "25-34",
        "EMAIL_MALE_35-44": "35-44",
        "EMAIL_MALE_45-54": "45-54",
        "EMAIL_MALE_55-64": "55-64",
        "EMAIL_MALE_65PLUS": "65PLUS",
        "FB_FEMALE_13-17": "13-17",
        "FB_FEMALE_18-24": "18-24",
        "FB_FEMALE_25-34": "25-34",
        "FB_FEMALE_35-44": "35-44",
        "FB_FEMALE_45-54": "45-54",
        "FB_FEMALE_55-64": "55-64",
        "FB_FEMALE_65PLUS": "65PLUS",
        "FB_MALE_13-17": "13-17",
        "FB_MALE_18-24": "18-24",
        "FB_MALE_25-34": "25-34",
        "FB_MALE_35-44": "35-44",
        "FB_MALE_45-54": "45-54",
        "FB_MALE_55-64": "55-64",
        "FB_MALE_65PLUS": "65PLUS",
        "PHONE_FEMALE_13-17": "13-17",
        "PHONE_FEMALE_18-24": "18-24",
        "PHONE_FEMALE_25-34": "25-34",
        "PHONE_FEMALE_35-44": "35-44",
        "PHONE_FEMALE_45-54": "45-54",
        "PHONE_FEMALE_55-64": "55-64",
        "PHONE_FEMALE_65PLUS": "65PLUS",
        "PHONE_MALE_13-17": "13-17",
        "PHONE_MALE_18-24": "18-24",
        "PHONE_MALE_25-34": "25-34",
        "PHONE_MALE_35-44": "35-44",
        "PHONE_MALE_45-54": "45-54",
        "PHONE_MALE_55-64": "55-64",
        "PHONE_MALE_65PLUS": "65PLUS",
        "FMD009V0051317HIGHSRMLDESTADS": "13-17",
        "FMD009V0051317SRMLDESTADS": "13-17",
        "FMD009V0051824HIGHSRMLDESTADS": "18-24",
        "FMD009V0051824SRMLDESTADS": "18-24",
        "FMD009V0052534HIGHSRMLDESTADS": "25-34",
        "FMD009V0052534SRMLDESTADS": "25-34",
        "FMD009V0053599HIGHSRMLDESTADS": "35-99",
        "FMD009V0053599SRMLDESTADS": "35-99",
        "MMD009V0051317HIGHSRMLDESTADS": "13-17",
        "MMD009V0051317SRMLDESTADS": "13-17",
        "MMD009V0051824HIGHSRMLDESTADS": "18-24",
        "MMD009V0051824SRMLDESTADS": "18-24",
        "MMD009V0052534HIGHSRMLDESTADS": "25-34",
        "MMD009V0052534SRMLDESTADS": "25-34",
        "MMD009V0053599HIGHSRMLDESTADS": "35-99",
        "MMD009V0053599SRMLDESTADS": "35-99",
    }
    if cohort is not None:
        for x in cohort.split('|'):
            if x in map_:
                return map_[x]
    return ''


@F.udf(returnType=StringType())
def unify_device(cohort):
    if cohort is not None:
        dc = {'A_15031263': '15-20K', 'A_94523754': '20-25K', 'A_40990869': '25-35K', 'A_21231588': '35K+'}
        for x in cohort.split('|'):
            if x in dc:
                return x
    return ''


@F.udf(returnType=MapType(keyType=StringType(), valueType=StringType()))
def cohort_enhance(cohort, ad_time, reach, cohort_col_name):
    global inventory_distribution, reach_distribution
    if cohort is None or cohort == "":
        res = {}
        for key in inventory_distribution[cohort_col_name]:
            cohort_inv = ad_time * inventory_distribution[cohort_col_name][key]
            cohort_reach = reach * reach_distribution[cohort_col_name][key]
            res[key] = f"{cohort_inv}#{cohort_reach}"
    else:
        res = {cohort: f"{ad_time}#{reach}"}
    return res


# unify regular cohort names
def unify_regular_cohort_names(df: DataFrame, group_cols, DATE):
    valid_matches = spark.read.parquet(MATCH_CMS_PATH_TEMPL % DATE) \
        .selectExpr('startdate as cd', 'content_id').distinct()
    regular_cohorts = ['gender', 'age', 'country', 'language', 'platform', 'nccs', 'device', 'city', 'state']
    unify_df = df\
        .join(valid_matches, ['cd', 'content_id'])\
        .withColumn('nccs', unify_nccs('cohort'))\
        .withColumn('device', unify_device('cohort'))\
        .withColumn('gender', unify_gender('cohort'))\
        .withColumn('age', unify_age('cohort')) \
        .groupby(*group_cols, *regular_cohorts) \
        .agg(F.sum('ad_time').alias('ad_time'), F.sum('reach').alias('reach'))\
        .cache()
    # print(unify_df.count())
    cohort = "gender"
    unify_df.where(f"{cohort} is not null and {cohort} != ''").groupby(cohort).agg(F.sum('ad_time').alias('ad_time'),
                                                                                   F.sum('reach').alias('reach'))
    # all_cols = unify_df.columns
    # global inventory_distribution, reach_distribution
    # inventory_distribution = {}
    # reach_distribution = {}
    # for cohort in regular_cohorts:
    #     dis = unify_df.where(f"{cohort} is not null and {cohort} != ''").groupby(cohort).agg(F.sum('ad_time').alias('ad_time'), F.sum('reach').alias('reach')).collect()
    #     inventory_distribution[cohort] = {}
    #     total_inv = 0.0
    #     total_reach = 0.0
    #     inventory_distribution[cohort] = {}
    #     reach_distribution[cohort] = {}
    #     for row in dis:
    #         inventory_distribution[cohort][row[0]] = float(row[1])
    #         reach_distribution[cohort][row[0]] = float(row[2])
    #         total_inv += float(row[1])
    #         total_reach += float(row[2])
    #     for key in inventory_distribution[cohort]:
    #         inventory_distribution[cohort][key] = inventory_distribution[cohort][key] / max(total_inv, 0.00001)
    #         reach_distribution[cohort][key] = reach_distribution[cohort][key] / max(total_reach, 0.00001)
    # print(inventory_distribution['gender'])
    # # print(reach_distribution)
    # print(reach_distribution['gender'])


cd = "2023-08-15"
last_cd = []
last_cd0 = get_last_cd(INVENTORY_SAMPLING_PATH, cd, 1000)  # recent 30 days on which there are matches
for x in last_cd0:
    last_cd.append(x)
    # if x.startswith("2022"):
    #     last_cd.append(x)

print(last_cd)
lst = [spark.read.parquet(f'{INVENTORY_SAMPLING_PATH}cd={i}').withColumn('cd', F.lit(i)) for i in last_cd]
regular_cohorts_df = reduce(lambda x, y: x.union(y), lst)
unify_regular_cohort_names(regular_cohorts_df, ['cd', 'content_id'], cd)

