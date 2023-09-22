import pandas as pd
import sys
import time
from functools import reduce
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.types import *

from util import *
from path import *

inventory_distribution = {}
reach_distribution = {}
cohort_cols = ['gender', 'age_bucket', 'city', 'language', 'state', 'location_cluster', 'pincode', 'interest', 'device_brand', 'device_model',
    'primary_sim', 'data_sim', 'platform', 'os', 'app_version', 'subscription_type', 'content_type', 'content_genre']


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
def unify_regular_cohort_names(unify_df: DataFrame, group_cols, DATE):
    all_cols = unify_df.columns
    global inventory_distribution, reach_distribution
    inventory_distribution = {}
    reach_distribution = {}
    for cohort in cohort_cols[:2]:
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
    print(inventory_distribution)
    print(reach_distribution)
    for cohort in cohort_cols[:2]:
        print(cohort)
        res_df = unify_df\
            .withColumn(cohort, cohort_enhance(cohort, 'ad_time', 'reach', F.lit(cohort)))\
            .select(*all_cols, F.explode(cohort)) \
            .drop(cohort)\
            .withColumnRenamed('key', cohort)\
            .withColumn('ad_time', F.element_at(F.split(F.col('value'), "#"), 1).cast("float"))\
            .withColumn('reach', F.element_at(F.split(F.col('value'), "#"), 2).cast("float"))\
            .drop('value') \
            .groupby(*group_cols, *cohort_cols) \
            .agg(F.sum('ad_time').alias('ad_time'), F.sum('reach').alias('reach'))
        save_data_frame(res_df, PREROLL_SAMPLING_ROOT_PATH + "cohort_tmp/" + cohort + f"/cd={DATE}")
        unify_df = load_data_frame(spark, PREROLL_SAMPLING_ROOT_PATH + "cohort_tmp/" + cohort + f"/cd={DATE}")
    save_data_frame(unify_df, PREROLL_SAMPLING_ROOT_PATH + "cohort_tmp/" + "all/" + f"/cd={DATE}")
    unify_df = load_data_frame(spark, PREROLL_SAMPLING_ROOT_PATH + "cohort_tmp/" + "all/" + f"/cd={DATE}").cache()
    print(unify_df.count())
    print("null of sampling filling done")
    return unify_df


def load_inventory(cd, n=15):
    last_cd = get_last_cd(PREROLL_SAMPLING_PATH, cd, n)
    lst = [spark.read.parquet(f'{PREROLL_SAMPLING_PATH}cd={i}').withColumn('cd', F.lit(i)) for i in last_cd]
    return reduce(lambda x, y: x.union(y), lst).fillna('', cohort_cols)


def moving_avg_factor_calculation_of_regular_cohorts(df, group_cols, lambda_=0.2):
    group_cols = [F.col(x).desc() for x in group_cols]
    print(group_cols)
    df4 = df.withColumn('factor', F.lit(lambda_) * F.pow(
            F.lit(1-lambda_),
            F.row_number().over(Window.partitionBy(*cohort_cols).orderBy(*group_cols))-F.lit(1)
        )
    )
    return df4.cache()


def moving_avg_calculation_of_regular_cohorts(df, group_cols, target):
    df2 = df.groupby(group_cols).agg(F.sum(target).alias('gsum'))
    df3 = df.join(df2, group_cols).withColumn(target, F.col(target)/F.col('gsum'))
    df4 = df3.withColumn(target, F.col(target) * F.col('factor'))
    return df4.groupby(cohort_cols).agg(F.sum(target).alias(target))


def combine_custom_cohort(df, cd, src_col='watch_time', dst_col='ad_time'):
    ch = pd.read_parquet(f'{CUSTOM_COHORT_PATH}cd={cd}/')
    ch = ch[~(ch.is_cricket==False)]
    ch.segments.fillna('', inplace=True)
    ch2 = (ch.groupby('segments')[src_col].sum().rename(dst_col).rename_axis('custom_cohorts') / ch[src_col].sum()).reset_index()
    df2 = df.crossJoin(F.broadcast(spark.createDataFrame(ch2).withColumnRenamed(dst_col, dst_col+"_c")))\
        .withColumn(dst_col, F.expr(f'{dst_col} * {dst_col}_c'))\
        .drop(dst_col+"_c")
    return df2


def main(cd):
    print(time.ctime())
    regular_cohorts_df = load_inventory(cd)
    regular_cohorts_df = unify_regular_cohort_names(regular_cohorts_df, ['cd'], cd)
    # inventory distribution prediction
    regular_cohorts_df_with_factor = moving_avg_factor_calculation_of_regular_cohorts(regular_cohorts_df, ['cd'])
    print(time.ctime())
    regular_cohort_inventory_df = moving_avg_calculation_of_regular_cohorts(regular_cohorts_df_with_factor, ['cd'], target='ad_time')
    save_data_frame(combine_custom_cohort(regular_cohort_inventory_df, cd, 'watch_time', 'ad_time'), f'{PREROLL_INVENTORY_RATIO_RESULT_PATH}cd={cd}')
    print(time.ctime())
    print("inventory sampling done")
    # reach distribution prediction
    regular_cohort_reach_df = moving_avg_calculation_of_regular_cohorts(regular_cohorts_df_with_factor, ['cd'], target='reach')
    save_data_frame(combine_custom_cohort(regular_cohort_reach_df, cd, 'reach', 'reach'), f'{PREROLL_REACH_RATIO_RESULT_PATH}cd={cd}')
    print(time.ctime())
    print("reach sampling done")


if __name__ == '__main__':
    DATE = sys.argv[1]
    main(DATE)
