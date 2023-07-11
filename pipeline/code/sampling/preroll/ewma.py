import pandas as pd
import sys
from functools import reduce
import pyspark.sql.functions as F
from common import *

cohort_cols = ['gender', 'age_bucket', 'city', 'state', 'location_cluster', 'pincode', 'interest', 'device_brand', 'device_model',
    'primary_sim', 'data_sim', 'platform', 'os', 'app_version', 'subscription_type', 'content_type', 'content_genre']


def load_inventory(cd, n=15):
    last_cd = get_last_cd(PREROLL_SAMPLING_PATH, cd, n)
    lst = [spark.read.parquet(f'{INVENTORY_SAMPLING_PATH}cd={i}').withColumn('cd', F.lit(i)) for i in last_cd]
    return reduce(lambda x,y: x.union(y), lst)


def moving_avg(df, group_cols, target, alpha=0.2):
    df2 = df.fillna('')  # this is important because pandas groupby will ignore null
    df2[target+'_ratio'] = df2[target] / df2.groupby(group_cols)[target].transform('sum')  # index=cd, cols=country, platform,..., target, target_ratio
    df3 = df2.pivot_table(index=group_cols, columns=cohort_cols, values=target+'_ratio', aggfunc='sum').fillna(0)  # index=cd, cols=cohort_candidate_combination1, cohort_candidate_combination2, ...
    # S[n+1] = (1-alpha) * S[n] + alpha * A[n+1]
    df4 = df3.ewm(alpha=alpha, adjust=False).mean().shift(1)
    return df4.iloc[-1].rename(target).reset_index()  # cols=country, platform,..., target


def merge_custom_cohort(df, cd, src_col='watch_time', dst_col='ad_time'):
    ch = pd.read_parquet(f'{CUSTOM_COHORT_PATH}cd={cd}/')
    ch = ch[~(ch.is_cricket==False)]
    ch.segments.fillna('', inplace=True)
    ch2 = (ch.groupby('segments')[src_col].sum().rename(dst_col).rename_axis('custom_cohorts') / ch[src_col].sum()).reset_index()
    df2 = df.merge(ch2, how='cross')
    df2[dst_col] = df2[dst_col+'_x'] * df2[dst_col+'_y']
    return df2.drop(columns=[dst_col+'_x', dst_col+'_y'])


if __name__ == '__main__':
    DATE = sys.argv[1]
    iv = load_inventory(DATE)
    df = moving_avg(iv, ['cd'], target='ad_time')
    ad_time = merge_custom_cohort(df, DATE)
    df2 = moving_avg(iv, ['cd'], target='reach')
    reach = merge_custom_cohort(df2, DATE, 'reach', 'reach')
    final = ad_time.merge(reach, on=['cd'] + cohort_cols)
    final.to_parquet(f'{PREROLL_RATIO_RESULT_PATH}cd={DATE}/p0.parquet')
