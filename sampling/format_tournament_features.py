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
# from config import *
#
# df = load_data_frame(spark, "")
# a = df.collect()
# set1 = {}
# set2 = {}
# for row in a:
#     date1 = row[0]
#     title1 = " vs ".join(sorted(row[1].strip().lower().split(" vs ")))
#     set1[title1] = date1
#     date2 = row[2]
#     title2 = " vs ".join(sorted([row[3].strip().lower().replace("netherlands", "west indies"), row[4].strip().lower().replace("netherlands", "west indies")]))
#     set2[title2] = date2
#
# df2 = load_hive_table(spark, "adtech.daily_predicted_vv_report").where('cd="2023-08-11"').cache()
# l = ["2023-09-05","2023-09-02","2023-09-03","2023-09-04","2023-08-31","2023-08-30","2023-09-06","2023-09-09","2023-09-10","2023-09-12","2023-09-14","2023-09-15","2023-09-17"]
# for row in a:
#     title = " vs ".join(sorted(row[1].strip().lower().split(" vs ")))
#     if title in set2:
#         l.append(set2[title])
#         print(set2[title])
#     else:
#         print("error")
#
#
# for d in l:
#     print(df2.where(f'ds="{d}"').select('free_vv').collect()[0][0])
#
# print("")
# for d in l:
#     print(df2.where(f'ds="{d}"').select('sub_vv').collect()[0][0])
#
# import pyspark.sql.functions as F
#
# df = spark.read.parquet(f'{FINAL_ALL_PREDICTION_PATH}cd=2023-08-15/').cache()
# for col in ['platform', 'ageBucket', 'city', 'state', 'devicePrice', 'gender', 'language']:
#     print(col)
#     df.where('matchId = "708501"').groupby(col).agg(F.count('reach'), F.sum('reach'), F.sum('inventory')).toPandas().to_csv(col+'.csv')
#
#
# for col in ['platform', 'ageBucket', 'city', 'state', 'devicePrice', 'gender', 'language']:
#     print(col)
#     # df\
#     #     .where('matchId = "708501"')\
#     #     .groupby(col).agg(F.count('reach'), F.sum('reach'), F.sum('inventory'), F.sum('inventory')/F.count('reach'))\
#     #     .show(20, False)
#
#
# df2.groupby('platform').count().show(20)
#
# df = df2.toPandas()
# target = 'ad_time'
# group_cols = ['cd']
# cohort_cols = ['country', 'platform', 'city', 'state', 'nccs', 'device', 'gender', 'age', 'language']
# # calculate the inventory/reach percentage for each cohort combination
# df[target+'_ratio'] = df[target] / df.groupby(group_cols)[target].transform('sum')  # index=cd, cols=country, platform,..., target, target_ratio
# # convert each cohort combination to one single column
# df.groupby('plaform').count()
# target_value_distribution_df = df.pivot_table(index=group_cols, columns=cohort_cols, values=target+'_ratio', aggfunc='sum').fillna(0)  # index=cd, cols=cohort_candidate_combination1, cohort_candidate_combination2, ...
# # S[n+1] = (1-alpha) * S[n] + alpha * A[n+1]
# res_df = target_value_distribution_df.ewm(alpha=alpha, adjust=False).mean().shift(1)
# # return the last row as the prediction results
# res_df.iloc[-1].rename(target).reset_index()  # cols=country, platform,..., target
#
# import sys
# from functools import reduce
#
# import pandas as pd
# import pyspark.sql.functions as F
# from pyspark.sql.types import *
#
# from util import *
# from path import *
#
# inventory_distribution = {}
# reach_distribution = {}
#
#
# @F.udf(returnType=StringType())
# def unify_nccs(cohort):
#     if cohort is not None:
#         for x in cohort.split('|'):
#             if x.startswith('NCCS_'):
#                 return x
#     return ''
#
#
# @F.udf(returnType=StringType())
# def unify_gender(cohort):
#     if cohort is not None:
#         for x in cohort.split('|'):
#             if x.startswith('FMD00') or '_FEMALE_' in x:
#                 return 'f'
#             if x.startswith('MMD00') or '_MALE_' in x:
#                 return 'm'
#     return ''
#
#
# @F.udf(returnType=StringType())
# def unify_age(cohort):
#     map_ = {
#         "EMAIL_FEMALE_13-17": "13-17",
#         "EMAIL_FEMALE_18-24": "18-24",
#         "EMAIL_FEMALE_25-34": "25-34",
#         "EMAIL_FEMALE_35-44": "35-44",
#         "EMAIL_FEMALE_45-54": "45-54",
#         "EMAIL_FEMALE_55-64": "55-64",
#         "EMAIL_FEMALE_65PLUS": "65PLUS",
#         "EMAIL_MALE_13-17": "13-17",
#         "EMAIL_MALE_18-24": "18-24",
#         "EMAIL_MALE_25-34": "25-34",
#         "EMAIL_MALE_35-44": "35-44",
#         "EMAIL_MALE_45-54": "45-54",
#         "EMAIL_MALE_55-64": "55-64",
#         "EMAIL_MALE_65PLUS": "65PLUS",
#         "FB_FEMALE_13-17": "13-17",
#         "FB_FEMALE_18-24": "18-24",
#         "FB_FEMALE_25-34": "25-34",
#         "FB_FEMALE_35-44": "35-44",
#         "FB_FEMALE_45-54": "45-54",
#         "FB_FEMALE_55-64": "55-64",
#         "FB_FEMALE_65PLUS": "65PLUS",
#         "FB_MALE_13-17": "13-17",
#         "FB_MALE_18-24": "18-24",
#         "FB_MALE_25-34": "25-34",
#         "FB_MALE_35-44": "35-44",
#         "FB_MALE_45-54": "45-54",
#         "FB_MALE_55-64": "55-64",
#         "FB_MALE_65PLUS": "65PLUS",
#         "PHONE_FEMALE_13-17": "13-17",
#         "PHONE_FEMALE_18-24": "18-24",
#         "PHONE_FEMALE_25-34": "25-34",
#         "PHONE_FEMALE_35-44": "35-44",
#         "PHONE_FEMALE_45-54": "45-54",
#         "PHONE_FEMALE_55-64": "55-64",
#         "PHONE_FEMALE_65PLUS": "65PLUS",
#         "PHONE_MALE_13-17": "13-17",
#         "PHONE_MALE_18-24": "18-24",
#         "PHONE_MALE_25-34": "25-34",
#         "PHONE_MALE_35-44": "35-44",
#         "PHONE_MALE_45-54": "45-54",
#         "PHONE_MALE_55-64": "55-64",
#         "PHONE_MALE_65PLUS": "65PLUS",
#         "FMD009V0051317HIGHSRMLDESTADS": "13-17",
#         "FMD009V0051317SRMLDESTADS": "13-17",
#         "FMD009V0051824HIGHSRMLDESTADS": "18-24",
#         "FMD009V0051824SRMLDESTADS": "18-24",
#         "FMD009V0052534HIGHSRMLDESTADS": "25-34",
#         "FMD009V0052534SRMLDESTADS": "25-34",
#         "FMD009V0053599HIGHSRMLDESTADS": "35-99",
#         "FMD009V0053599SRMLDESTADS": "35-99",
#         "MMD009V0051317HIGHSRMLDESTADS": "13-17",
#         "MMD009V0051317SRMLDESTADS": "13-17",
#         "MMD009V0051824HIGHSRMLDESTADS": "18-24",
#         "MMD009V0051824SRMLDESTADS": "18-24",
#         "MMD009V0052534HIGHSRMLDESTADS": "25-34",
#         "MMD009V0052534SRMLDESTADS": "25-34",
#         "MMD009V0053599HIGHSRMLDESTADS": "35-99",
#         "MMD009V0053599SRMLDESTADS": "35-99",
#     }
#     if cohort is not None:
#         for x in cohort.split('|'):
#             if x in map_:
#                 return map_[x]
#     return ''
#
#
# @F.udf(returnType=StringType())
# def unify_device(cohort):
#     if cohort is not None:
#         dc = {'A_15031263': '15-20K', 'A_94523754': '20-25K', 'A_40990869': '25-35K', 'A_21231588': '35K+'}
#         for x in cohort.split('|'):
#             if x in dc:
#                 return x
#     return ''
#
#
# @F.udf(returnType=MapType(keyType=StringType(), valueType=StringType()))
# def cohort_enhance(cohort, ad_time, reach, cohort_col_name):
#     global inventory_distribution, reach_distribution
#     if cohort is None or cohort == "":
#         res = {}
#         for key in inventory_distribution[cohort_col_name]:
#             cohort_inv = ad_time * inventory_distribution[cohort_col_name][key]
#             cohort_reach = reach * reach_distribution[cohort_col_name][key]
#             res[key] = f"{cohort_inv}#{cohort_reach}"
#     else:
#         res = {cohort: f"{ad_time}#{reach}"}
#     return res
#
#
# # unify regular cohort names
# def unify_regular_cohort_names(df: DataFrame, group_cols, DATE):
#     valid_matches = spark.read.parquet(MATCH_CMS_PATH_TEMPL % DATE) \
#         .selectExpr('startdate as cd', 'content_id').distinct()
#     regular_cohorts = ['gender', 'age', 'country', 'language', 'platform', 'nccs', 'device', 'city', 'state']
#     unify_df = df\
#         .join(valid_matches, ['cd', 'content_id'])\
#         .withColumn('nccs', unify_nccs('cohort'))\
#         .withColumn('device', unify_device('cohort'))\
#         .withColumn('gender', unify_gender('cohort'))\
#         .withColumn('age', unify_age('cohort')) \
#         .groupby(*group_cols, *regular_cohorts) \
#         .agg(F.sum('ad_time').alias('ad_time'), F.sum('reach').alias('reach'))\
#         .cache()
#     # print(unify_df.count())
#     cohort = "gender"
#     unify_df\
#         .where(f"{cohort} is not null and {cohort} != ''")\
#         .groupby('cd', 'content_id', cohort)\
#         .agg(F.sum('ad_time').alias('ad_time'),
#              F.sum('reach').alias('reach'))\
#         .orderBy('cd', 'content_id')\
#         .show(1000, False)
#     unify_df \
#         .where(f"{cohort} is not null and {cohort} != ''") \
#         .groupby('cd', cohort) \
#         .agg(F.sum('ad_time').alias('ad_time'),
#              F.sum('reach').alias('reach')) \
#         .orderBy('cd', 'content_id') \
#         .show(1000, False)
#     # all_cols = unify_df.columns
#     # global inventory_distribution, reach_distribution
#     # inventory_distribution = {}
#     # reach_distribution = {}
#     # for cohort in regular_cohorts:
#     #     dis = unify_df.where(f"{cohort} is not null and {cohort} != ''").groupby(cohort).agg(F.sum('ad_time').alias('ad_time'), F.sum('reach').alias('reach')).collect()
#     #     inventory_distribution[cohort] = {}
#     #     total_inv = 0.0
#     #     total_reach = 0.0
#     #     inventory_distribution[cohort] = {}
#     #     reach_distribution[cohort] = {}
#     #     for row in dis:
#     #         inventory_distribution[cohort][row[0]] = float(row[1])
#     #         reach_distribution[cohort][row[0]] = float(row[2])
#     #         total_inv += float(row[1])
#     #         total_reach += float(row[2])
#     #     for key in inventory_distribution[cohort]:
#     #         inventory_distribution[cohort][key] = inventory_distribution[cohort][key] / max(total_inv, 0.00001)
#     #         reach_distribution[cohort][key] = reach_distribution[cohort][key] / max(total_reach, 0.00001)
#     # print(inventory_distribution['gender'])
#     # # print(reach_distribution)
#     # print(reach_distribution['gender'])
#
#
# cd = "2023-08-15"
# last_cd = []
# last_cd0 = get_last_cd(INVENTORY_SAMPLING_PATH, cd, 1000)  # recent 30 days on which there are matches
# for x in last_cd0:
#     last_cd.append(x)
#     # if x.startswith("2022"):
#     #     last_cd.append(x)
#
# print(last_cd)
# lst = [spark.read.parquet(f'{INVENTORY_SAMPLING_PATH}cd={i}').withColumn('cd', F.lit(i)) for i in last_cd]
# df = reduce(lambda x, y: x.union(y), lst)
# # unify_regular_cohort_names(regular_cohorts_df, ['cd', 'content_id'], cd)
# group_cols = ['cd', 'content_id']
# valid_matches = spark.read.parquet(MATCH_CMS_PATH_TEMPL % cd) \
#         .selectExpr('startdate as cd', 'content_id').distinct()
# regular_cohorts = ['gender', 'age', 'country', 'language', 'platform', 'nccs', 'device', 'city', 'state']
# unify_df = df\
#     .join(valid_matches, ['cd', 'content_id'])\
#     .withColumn('nccs', unify_nccs('cohort'))\
#     .withColumn('device', unify_device('cohort'))\
#     .withColumn('gender', unify_gender('cohort'))\
#     .withColumn('age', unify_age('cohort')) \
#     .groupby(*group_cols, *regular_cohorts) \
#     .agg(F.sum('ad_time').alias('ad_time'), F.sum('reach').alias('reach'))\
#     .cache()
# # print(unify_df.count())
# cohort = "gender"
# unify_df\
#     .where(f"{cohort} is not null and {cohort} != ''")\
#     .groupby('cd', 'content_id', cohort)\
#     .agg(F.sum('ad_time').alias('ad_time'),
#          F.sum('reach').alias('reach'))\
#     .orderBy('cd', 'content_id', cohort)\
#     .show(1000, False)
#
# unify_df \
#     .where(f"{cohort} is not null and {cohort} != ''") \
#     .groupby('cd', cohort) \
#     .agg(F.sum('ad_time').alias('ad_time'),
#          F.sum('reach').alias('reach')) \
#     .orderBy('cd', cohort) \
#     .show(1000, False)
#
#
# spark.stop()
# spark = hive_spark("etl")
# base_cid = 1540018975
# date = "2022-10-21"
# wv = load_data_frame(spark, f"s3://hotstar-ads-ml-us-east-1-prod/data_exploration/data/data_backup/watched_video/cd={date}") \
#     .where(f'content_id="{base_cid}"')\
#     .select('dw_d_id', 'content_id', 'user_segments')\
#     .withColumn('cohort', parse_wv_segments('user_segments'))\
#     .withColumn('age', unify_age('cohort'))\
#     .withColumn('gender', unify_gender('cohort'))\
#     .cache()
#
# print(wv.count())
# wv.show(10, False)
# wv.groupby('gender').agg(F.expr('count(distinct dw_d_id) as reach')).show()
# wv.groupby('age').agg(F.expr('count(distinct dw_d_id) as reach')).show()
#

import sys
from functools import reduce

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import *

from util import *
from path import *


def load_regular_cohorts_data(cd, n=30) -> DataFrame:
    last_cd = get_last_cd(INVENTORY_SAMPLING_PATH, cd, n)  # recent 30 days on which there are matches
    print(last_cd)
    lst = [spark.read.parquet(f'{INVENTORY_SAMPLING_PATH}cd={i}').withColumn('cd', F.lit(i)) for i in last_cd]
    return reduce(lambda x, y: x.union(y), lst)


date = "2023-08-30"
spark.stop()
spark = hive_spark("etl")
df = spark.createDataFrame([('1970-01-02 00:00:00', '1540024245')], ['ts', 'content_id'])
content_ids = load_regular_cohorts_data(cd, n=4).cache()
content_ids.select('cd', 'content_id').distinct().show(10, False)
dates = [item[0] for item in content_ids.select('cd').distinct().collect()]
print(dates)
new_match_df = []
for date in dates:
    print(date)
    new_match_df.append(add_labels_to_new_matches(spark, date, content_ids.where(f'cd="{date}"')))


res = reduce(lambda x, y: x.union(y), new_match_df).cache()
res.drop('playout_id', 'language', 'platform', 'country', 'city', 'state', 'cohort', 'ad_time', 'reach').distinct().show(100, False)



save_data_frame(res.drop('playout_id', 'language', 'platform', 'country', 'city', 'state', 'cohort', 'ad_time', 'reach').distinct(), PIPELINE_BASE_PATH+"/inventory_label/icc_world_test_championship")

import sys

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import *

from util import *
from path import *
from config import *


def make_segment_str(lst):
    filtered = set()
    equals = ['A_15031263', 'A_94523754', 'A_40990869', 'A_21231588']  # device price
    prefixs = ['NCCS_', 'CITY_', 'STATE_', 'FMD00', 'MMD00', 'P_', 'R_F', 'R_M']
    middles = ['_MALE_', '_FEMALE_']
    for t in lst:
        match = False
        for s in equals:
            if t == s:
                match = True
                break
        if not match:
            for s in prefixs:
                if t.startswith(s):
                    match = True
                    break
        if not match:
            for s in middles:
                if s in t:
                    match = True
                    break
        if match:
            filtered.add(t)
    return '|'.join(sorted(filtered))


@F.udf(returnType=StringType())
def parse_preroll_segment(lst):
    if lst is None:
        return None
    if type(lst) == str:
        lst = lst.split(",")
    return make_segment_str(lst)


@F.udf(returnType=StringType())
def parse_wv_segments(segments):
    if segments is None:
        return None
    try:
        js = json.loads(segments)
    except:
        return None
    if type(js) == list:
        lst = js
    elif type(js) == dict:
        lst = js.get('data', [])
    else:
        return None
    return make_segment_str(lst)


@F.udf(returnType=TimestampType())
def parse_timestamp(date: str, ts: str):
    return pd.Timestamp(date + ' ' + ts, tz='asia/kolkata')


def preprocess_playout(df):
    return df\
        .withColumn('break_start', parse_timestamp('Start Date', 'Start Time')) \
        .withColumn('break_end', parse_timestamp('End Date', 'End Time')) \
        .selectExpr(
            '`Content ID` as content_id',
            'trim(lower(`Playout ID`)) as playout_id',
            'trim(lower(Language)) as language',
            'trim(lower(Tenant)) as country',
            'explode(split(trim(lower(Platform)), "\\\\|")) as platform',
            'break_start',
            'break_end',
        )\
        .where('break_start is not null and break_end is not null') \
        .withColumn('break_start', F.expr('cast(break_start as long)')) \
        .withColumn('break_end', F.expr('cast(break_end as long)'))


spark.stop()
spark = hive_spark('etl')
# # s = """SELECT count(distinct dw_d_id) AS reach, SUM(ad_inventory) AS preroll_inventory, content_type,substring(demo_gender, 1, 2) AS demo_gender,content_id
# # FROM adtech.pk_inventory_metrics_daily
# # WHERE ad_placement='Preroll' and content_id in ('1540018945','1540018948','1540019085','1540019088','1540018957','1540018960','1540019091','1540019094','1540018969','1540018972','1540018975','1540018978','1540018981','1540018984','1540018987','1540018990','1540018993','1540018996','1540018999','1540019002','1540019008','1540019011','1540019097','1540019020','1540019023','1540019026','1540019100','1540019029','1540019032','1540019035','1540019038','1540019041','1540019044','1540019047','1540019050','1540019053','1540019056','1540019059','1540019103','1540019062','1540019065','1540019068')
# # AND content_type='SPORT_LIVE'
# # AND cd>= date '2022-10-16'
# # AND cd<= date '2022-11-14'
# # GROUP BY content_type,4,content_id
# # order by content_id"""
# # spark.sql(s).show(1000, False)
# s = """SELECT dw_d_id, substring(demo_gender, 1, 2) AS demo_gender
# FROM adtech.pk_inventory_metrics_daily
# WHERE ad_placement='Preroll' and content_id in ('1540018945','1540018948','1540019085','1540019088','1540018957','1540018960','1540019091','1540019094','1540018969','1540018972','1540018975','1540018978','1540018981','1540018984','1540018987','1540018990','1540018993','1540018996','1540018999','1540019002','1540019008','1540019011','1540019097','1540019020','1540019023','1540019026','1540019100','1540019029','1540019032','1540019035','1540019038','1540019041','1540019044','1540019047','1540019050','1540019053','1540019056','1540019059','1540019103','1540019062','1540019065','1540019068')
# AND content_type='SPORT_LIVE'
# AND cd>= date '2022-10-16'
# AND cd<= date '2022-11-14'
# """
# save_data_frame(spark.sql(s), f"{PIPELINE_BASE_PATH}/data_tmp/gender_analysis/pk_inventory_metrics_daily/wc2022")


# R_F1317,R_F1824,R_F2534,R_F3599
# R_M1317,R_M1824,R_M2534,R_M3599

@F.udf(returnType=StringType())
def unify_gender(cohort):
    if cohort is not None:
        for x in cohort.split('|'):
            if x.startswith('FMD00') or '_FEMALE_' in x:
                return 'f'
            if x.startswith('MMD00') or '_MALE_' in x:
                return 'm'
            if x.startswith('R_F'):
                return 'f_from_random'
            if x.startswith('R_M'):
                return 'm_from_random'
    return ''


from functools import reduce

watch_video_sampled_path = "s3://hotstar-ads-ml-us-east-1-prod/data_exploration/data/data_backup/watched_video/"
wt = reduce(lambda x, y: x.union(y), [load_data_frame(spark, f"{watch_video_sampled_path}cd={date}").select('dw_d_id','content_id', 'user_segments')
                                 for date in get_date_list("2022-10-16", 30)])\
    .where("content_id in ('1540018945','1540018948','1540019085','1540019088','1540018957','1540018960','1540019091','1540019094','1540018969','1540018972','1540018975','1540018978','1540018981','1540018984','1540018987','1540018990','1540018993','1540018996','1540018999','1540019002','1540019008','1540019011','1540019097','1540019020','1540019023','1540019026','1540019100','1540019029','1540019032','1540019035','1540019038','1540019041','1540019044','1540019047','1540019050','1540019053','1540019056','1540019059','1540019103','1540019062','1540019065','1540019068')")\
    .withColumn('cohort', parse_wv_segments('user_segments'))\
    .withColumn('gender', unify_gender('cohort')).cache()

wt.groupby('content_id', 'gender')\
    .agg(F.countDistinct('dw_d_id'))\
    .show(2000, False)

wt.groupby('gender')\
    .agg(F.countDistinct('dw_d_id'))\
    .show(2000, False)

save_data_frame(wt, f"{PIPELINE_BASE_PATH}/data_tmp/gender_analysis/sampled_wv/wc2022")

old_df = load_data_frame(spark, "s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_v2/inventory_back_up_2023_08-23/cd=2023-06-11/").cache()

df=load_data_frame(spark, "s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_v2/inventory/cd=2023-06-11/").cache()

for col in ['playout_id', 'language', 'platform', 'country', 'city', 'state', 'cohort']:
    old_df.groupby(col).count().orderBy(col).show(30, False)
    df.groupby(col).count().orderBy(col).show(30, False)

old_df.where('cohort = "A_15031263|CITY_?|NCCS_A|PHONE_BARC_MALE_15-21|PHONE_MALE_18-24|PHONE_MALE_TV_15-21|STATE_?"').show(20, False)
df.where('cohort = "A_15031263|CITY_?|NCCS_A|PHONE_BARC_MALE_15-21|PHONE_MALE_18-24|PHONE_MALE_TV_15-21|STATE_?"').show(20, False)


spark.sql(f'select * from {WV_TABLE} where cd = "2023-06-11"') \
    .where(F.col('dw_p_id').substr(-1, 1).isin(['2', 'a', 'e', '8']))\
    .groupby(F.expr('lower(language)'))\
    .count()\
    .show(500, False)

# +----------------+---------+
# |lower(language) |count    |
# +----------------+---------+
# |unknown language|856      |
# |italian         |71       |
# |hindi           |19515625 |
# |serbian         |270      |
# |malayalam       |865402   |
# |മലയാളം          |5        |
# |bengali         |2123700  |
# |turkish         |2031     |
# |french          |145      |
# |japanese        |15381    |
# |null            |141030696|
# |marathi         |1075672  |
# |english         |1925966  |
# |hindi 100%      |2446     |
# |odia            |7328     |
# |na              |18       |
# |tamil           |3335050  |
# |kannada         |303684   |
# |bangla          |4        |
# |korean          |59000    |
# |                |3219     |
# |spanish         |408      |
# |estonian        |45       |
# |telugu          |2758676  |
# +----------------+---------+
spark.sql(f'select * from {WV_TABLE} where cd = "2023-06-11"') \
    .where(F.col('dw_p_id').substr(-1, 1).isin(['2', 'a', 'e', '8']))\
    .groupby(F.expr('lower(audio_language)'))\
    .count()\
    .show(500, False)

run_date = "2023-08-31"
cms_df = load_data_frame(spark, MATCH_CMS_PATH_TEMPL % run_date).selectExpr('content_id', 'startdate', 'lower(title)').collect()
cid_mapping = {}

for row in cms_df:
    if row[1] not in cid_mapping:
        cid_mapping[row[1]] = []
    cid_mapping[row[1]].append([row[0], row[2]])


@F.udf(returnType=StringType())
def get_cms_content_id(date, team1, team2, raw_content_id):
    global cid_mapping
    if date in cid_mapping:
        for match in cid_mapping[date]:
            if f"{team1} vs {team2}" in match[1] or f"{team2} vs {team1}" in match[1]:
                return match[0]
    return raw_content_id


load_data_frame(spark, PREDICTION_MATCH_TABLE_PATH + f"/cd=2023-08-21")\
    .withColumn('new_cid', get_cms_content_id('date', 'team1', 'team2', 'content_id'))\
    .select('date', 'team1', 'team2', 'content_id', 'new_cid')\
    .show(20, False)




run_date = "2023-08-31"
the_day_before_run_date = get_date_list(run_date, -2)[0]
gt_dau_df = load_data_frame(spark, f'{DAU_TRUTH_PATH}cd={run_date}/').withColumnRenamed('ds', 'date').cache()
gt_inv_df = load_data_frame(spark, f'{TRAIN_MATCH_TABLE_PATH}/cd={run_date}/')\
    .where(f'date="{the_day_before_run_date}"')\
    .selectExpr('date', 'content_id', *LABEL_COLS)\
    .withColumn('overall_vv', F.expr('match_active_free_num+match_active_sub_num'))\
    .withColumn('avod_wt', F.expr('match_active_free_num*watch_time_per_free_per_match'))\
    .withColumn('svod_wt', F.expr('match_active_sub_num*watch_time_per_subscriber_per_match'))\
    .withColumn('overall_wt', F.expr('avod_wt+svod_wt'))\
    .withColumn('avod_reach', F.expr('total_reach*(match_active_free_num/(match_active_free_num+match_active_sub_num))'))\
    .withColumn('svod_reach', F.expr('total_reach*(match_active_sub_num/(match_active_free_num+match_active_sub_num))'))\
    .join(gt_dau_df, 'date')\
    .selectExpr('date', 'content_id', 'vv as overall_dau', 'free_vv as avod_dau', 'sub_vv as svod_dau',
                'overall_vv', 'match_active_free_num as avod_vv', 'match_active_sub_num as svod_vv',
                'overall_wt', 'avod_wt', 'svod_wt', 'total_inventory',
                'total_reach', 'avod_reach', 'svod_reach')\
    .cache()
cols = gt_inv_df.columns[2:]
for col in cols:
    gt_inv_df = gt_inv_df.withColumn(col, F.expr(f'{col} / 1000000.0'))

gt_inv_df.show(20, False)

# factor = 1.0
factor = 1.3
predict_dau_df = load_data_frame(spark, f'{DAU_FORECAST_PATH}cd={the_day_before_run_date}/')\
    .withColumnRenamed('ds', 'date')\
    .withColumn('vv', F.expr(f"vv * {factor}"))\
    .withColumn('free_vv', F.expr(f"free_vv * {factor}"))\
    .withColumn('sub_vv', F.expr(f"vv - free_vv"))\
    .cache()
predict_inv_df = load_data_frame(spark, f'{TOTAL_INVENTORY_PREDICTION_PATH}/cd=2023-08-21/')\
    .where(f'date="{the_day_before_run_date}"')\
    .withColumn('overall_vv', F.expr('estimated_reach/0.85'))\
    .withColumn('avod_vv', F.expr('estimated_free_match_number/0.85'))\
    .withColumn('svod_vv', F.expr('estimated_sub_match_number/0.85'))\
    .withColumn('avod_wt', F.expr('estimated_free_match_number * estimated_watch_time_per_free_per_match'))\
    .withColumn('svod_wt', F.expr('estimated_sub_match_number * estimated_watch_time_per_subscriber_per_match'))\
    .withColumn('overall_wt', F.expr('avod_wt+svod_wt'))\
    .join(predict_dau_df, 'date')\
    .selectExpr('date', 'content_id', 'vv as overall_dau', 'free_vv as avod_dau', 'sub_vv as svod_dau',
                'overall_vv', 'avod_vv', 'svod_vv',
                'overall_wt', 'avod_wt', 'svod_wt', 'estimated_inventory as total_inventory',
                'estimated_reach as total_reach', 'estimated_free_match_number as avod_reach', 'estimated_sub_match_number as svod_reach')
cols = predict_inv_df.columns[2:]
for col in cols:
    predict_inv_df = predict_inv_df.withColumn(col, F.expr(f'{col} / 1000000.0'))

predict_inv_df.show(20, False)

# get break list with break_start_time, break_end_time
def break_info_processing(playout_df, date):
    cols = ['content_id', 'break_start_time_int', 'break_end_time_int']
    playout_df = playout_df \
        .withColumn('rank', F.expr('row_number() over (partition by content_id order by break_start_time_int)')) \
        .withColumn('rank_next', F.expr('rank+1'))
    res_df = playout_df \
        .join(playout_df.selectExpr('content_id', 'rank_next as rank', 'break_end_time_int as break_end_time_int_next'),
              ['content_id', 'rank']) \
        .withColumn('bias', F.expr('break_start_time_int - break_end_time_int_next')) \
        .where('bias >= 0') \
        .orderBy('break_start_time_int')
    res_df = playout_df \
        .where('rank = 1') \
        .select(*cols) \
        .union(res_df.select(*cols))
    # save_data_frame(res_df, PIPELINE_BASE_PATH + f"/label/break_info/cd={date}")
    # res_df = load_data_frame(spark, PIPELINE_BASE_PATH + f"/label/break_info/cd={date}")
    print(res_df.count())
    res_df.groupby('content_id').agg(F.sum('break_start_time_int'),F.sum('break_start_time_int')).show(20)
    return res_df


playout_df = load_data_frame(spark, PIPELINE_BASE_PATH + '/label' + PLAYOUT_LOG_PATH_SUFFIX + f"/cd={the_day_before_run_date}").cache()
break_info_processing(playout_df, the_day_before_run_date)
