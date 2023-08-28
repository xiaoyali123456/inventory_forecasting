import pandas as pd
import sys
from functools import reduce
import pyspark.sql.functions as F
from pyspark.sql.window import Window

from util import *
from path import *


cohort_cols = ['gender', 'age_bucket', 'city', 'language', 'state', 'location_cluster', 'pincode', 'interest', 'device_brand', 'device_model',
    'primary_sim', 'data_sim', 'platform', 'os', 'app_version', 'subscription_type', 'content_type', 'content_genre']


def load_inventory(cd, n=15):
    last_cd = get_last_cd(PREROLL_SAMPLING_PATH, cd, n)
    lst = [spark.read.parquet(f'{PREROLL_SAMPLING_PATH}cd={i}').withColumn('cd', F.lit(i)) for i in last_cd]
    return reduce(lambda x, y: x.union(y), lst)


def moving_avg_calculation_of_regular_cohorts(df, group_cols, target, lambda_=0.8):
    df2 = df.groupby(group_cols).agg(F.sum(target).alias('gsum'))
    df3 = df.join(df2, group_cols).withColumn(target, F.col(target)/F.col('gsum'))
    group_cols = [F.col(x).desc() for x in group_cols]
    df4 = df3.withColumn(target, F.col(target) * F.lit(lambda_) * F.pow(
            F.lit(1-lambda_),
            F.row_number().over(Window.partitionBy(*cohort_cols).orderBy(*group_cols))-F.lit(1)
        )
    )
    return df4.groupby(cohort_cols).agg(F.sum(target).alias(target))


def combine_custom_cohort(df, cd, src_col='watch_time', dst_col='ad_time'):
    ch = pd.read_parquet(f'{CUSTOM_COHORT_PATH}cd={cd}/')
    ch = ch[~(ch.is_cricket==False)]
    ch.segments.fillna('', inplace=True)
    ch2 = (ch.groupby('segments')[src_col].sum().rename(dst_col).rename_axis('custom_cohorts') / ch[src_col].sum()).reset_index()
    df2 = df.merge(ch2, how='cross')
    df2[dst_col] = df2[dst_col+'_x'] * df2[dst_col+'_y']
    return df2.drop(columns=[dst_col+'_x', dst_col+'_y'])


def main(cd):
    regular_cohorts_df = load_inventory(cd)
    # inventory distribution prediction
    regular_cohort_inventory_df = moving_avg_calculation_of_regular_cohorts(regular_cohorts_df, ['cd'], target='ad_time')
    combine_custom_cohort(regular_cohort_inventory_df, cd, 'watch_time', 'ad_time').to_parquet(f'{PREROLL_INVENTORY_RATIO_RESULT_PATH}cd={cd}/p0.parquet')
    print("inventory sampling done")
    # reach distribution prediction
    regular_cohort_reach_df = moving_avg_calculation_of_regular_cohorts(regular_cohorts_df, ['cd'], target='reach')
    combine_custom_cohort(regular_cohort_reach_df, cd, 'reach', 'reach').to_parquet(f'{PREROLL_REACH_RATIO_RESULT_PATH}cd={cd}/p0.parquet')
    print("reach sampling done")


if __name__ == '__main__':
    DATE = sys.argv[1]
    main(DATE)
