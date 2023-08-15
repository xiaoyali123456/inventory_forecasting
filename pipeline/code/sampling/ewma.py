"""
    1. aggregate regular cohorts distribution to generate table from recent 30 days on which there are matches
    ('cd', 'language', 'platform', 'country', 'city', 'state', 'nccs', 'device', 'gender', 'age', ad_time', 'reach')
    2. calculate inventory/reach rate of each regular cohorts
    3. use moving avg method to predict inventory/reach rate for each regular cohorts
    4. calculate inventory/reach rate of each custom cohorts
    5. merge the outputs of 3 and 4 to get the final result
"""
import sys
from functools import reduce

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import *

from util import *
from path import *

inventory_distribution = {}
reach_distribution = {}


def load_regular_cohorts_data(cd, n=30):
    last_cd = get_last_cd(INVENTORY_SAMPLING_PATH, cd, n)  # recent 30 days on which there are matches
    print(last_cd)
    lst = [spark.read.parquet(f'{INVENTORY_SAMPLING_PATH}cd={i}').withColumn('cd', F.lit(i)) for i in last_cd]
    return reduce(lambda x, y: x.union(y), lst)


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
    all_cols = unify_df.columns
    global inventory_distribution, reach_distribution
    inventory_distribution = {}
    reach_distribution = {}
    for cohort in regular_cohorts:
        dis = unify_df.where(f"{cohort} is not null and {cohort} != ''").groupby(cohort).agg(F.sum('ad_time').alias('ad_time'), F.sum('reach').alias('reach')).collect()
        inventory_distribution[cohort] = {}
        total_inv = 0.0
        total_reach = 0.0
        inventory_distribution[cohort] = {}
        reach_distribution[cohort] = {}
        for row in dis:
            inventory_distribution[cohort][row[0]] = float(row[1])
            reach_distribution[cohort][row[0]] = float(row[2])
            total_inv += float(row[1])
            total_reach += float(row[2])
        for key in inventory_distribution[cohort]:
            inventory_distribution[cohort][key] = inventory_distribution[cohort][key] / max(total_inv, 0.00001)
            reach_distribution[cohort][key] = reach_distribution[cohort][key] / max(total_reach, 0.00001)
    # print(inventory_distribution)
    # print(reach_distribution)
    for cohort in regular_cohorts[:-3]:
        print(cohort)
        res_df = unify_df\
            .withColumn(cohort, cohort_enhance(cohort, 'ad_time', 'reach', F.lit(cohort)))\
            .select(*all_cols, F.explode(cohort)) \
            .drop(cohort)\
            .withColumnRenamed('key', cohort)\
            .withColumn('ad_time', F.element_at(F.split(F.col('value'), "#"), 1).cast("float"))\
            .withColumn('reach', F.element_at(F.split(F.col('value'), "#"), 2).cast("float"))\
            .drop('value') \
            .groupby(*group_cols, *regular_cohorts) \
            .agg(F.sum('ad_time').alias('ad_time'), F.sum('reach').alias('reach'))
        save_data_frame(res_df, SAMPLING_ROOT_PATH + "cohort_tmp/" + cohort + f"/cd={DATE}")
        unify_df = load_data_frame(spark, SAMPLING_ROOT_PATH + "cohort_tmp/" + cohort + f"/cd={DATE}")
    return unify_df.groupby(
            *group_cols,
            *regular_cohorts)\
        .agg(F.sum('ad_time').alias('ad_time'), F.sum('reach').alias('reach'))\
        .toPandas()\
        .fillna('')


def moving_avg_calculation_of_regular_cohorts(df, group_cols, target, alpha=0.2):
    cohort_cols = ['country', 'platform', 'city', 'state', 'nccs', 'device', 'gender', 'age', 'language']
    # calculate the inventory/reach percentage for each cohort combination
    df[target+'_ratio'] = df[target] / df.groupby(group_cols)[target].transform('sum')  # index=cd, cols=country, platform,..., target, target_ratio
    # convert each cohort combination to one single column
    target_value_distribution_df = df.pivot_table(index=group_cols, columns=cohort_cols, values=target+'_ratio', aggfunc='sum').fillna(0)  # index=cd, cols=cohort_candidate_combination1, cohort_candidate_combination2, ...
    # S[n+1] = (1-alpha) * S[n] + alpha * A[n+1]
    res_df = target_value_distribution_df.ewm(alpha=alpha, adjust=False).mean().shift(1)
    # return the last row as the prediction results
    return res_df.iloc[-1].rename(target).reset_index()  # cols=country, platform,..., target


def combine_custom_cohort(regular_cohort_df, cd, src_col, dst_col):
    custom_cohort_df = pd.read_parquet(f'{CUSTOM_COHORT_PATH}cd={cd}/')
    custom_cohort_df = custom_cohort_df[~(custom_cohort_df.is_cricket==False)]
    custom_cohort_df.segments.fillna('', inplace=True)
    # calculate the inventory/reach percentage for each cohort
    custom_cohort_df = (custom_cohort_df.groupby('segments')[src_col].sum().rename(dst_col).rename_axis('custom_cohorts')
                        / custom_cohort_df[src_col].sum()).reset_index()
    # merge regular cohorts and custom cohorts
    combine_df = regular_cohort_df.merge(custom_cohort_df, how='cross')
    combine_df[dst_col] = combine_df[dst_col+'_x'] * combine_df[dst_col+'_y']
    return combine_df.drop(columns=[dst_col+'_x', dst_col+'_y'])  # schema: country, platform,..., custom_cohorts, target


def main(cd):
    regular_cohorts_df = load_regular_cohorts_data(cd)
    unified_regular_cohorts_df = unify_regular_cohort_names(regular_cohorts_df, ['cd', 'content_id'], cd)
    # print(len(unified_regular_cohorts_df))
    # inventory distribution prediction
    regular_cohort_inventory_df = moving_avg_calculation_of_regular_cohorts(unified_regular_cohorts_df, ['cd'], target='ad_time')
    combine_custom_cohort(regular_cohort_inventory_df, cd, 'watch_time', 'ad_time').to_parquet(f'{AD_TIME_SAMPLING_PATH}cd={cd}/p0.parquet')
    # reach distribution prediction
    regular_cohort_reach_df = moving_avg_calculation_of_regular_cohorts(unified_regular_cohorts_df, ['cd'],target='reach')
    combine_custom_cohort(regular_cohort_reach_df, cd, 'reach', 'reach').to_parquet(f'{REACH_SAMPLING_PATH}cd={cd}/p0.parquet')


if __name__ == '__main__':
    DATE = sys.argv[1]
    main(DATE)
