import pandas as pd
import sys
from functools import reduce
import pyspark.sql.functions as F
from common import *

cohort_cols = ['gender', 'age_bucket', 'city', 'state', 'location_cluster', 'pincode', 'interest', 'device_brand', 'device_model',
    'primary_sim', 'data_sim', 'platform', 'os', 'app_version', 'subscription_type', 'content_type', 'content_genre']


def load_inventory(cd, n=15):
    last_cd = get_last_cd(PREROLL_SAMPLING_PATH, cd, n)
    lst = [spark.read.parquet(f'{PREROLL_SAMPLING_PATH}cd={i}').withColumn('cd', F.lit(i)) for i in last_cd]
    return reduce(lambda x,y: x.union(y), lst)


def average(df, group_cols, targets = ['inventory', 'reach']):
    res = df.groupby(group_cols).agg(*[F.sum(x).alias(x) for x in targets]).toPandas()
    return res


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
    df = average(iv, ['cd'], targets=['ad_time', 'reach'])
    final = merge_custom_cohort(df, DATE, 'reach', 'reach')
    final.to_parquet(f'{PREROLL_RATIO_RESULT_PATH}cd={DATE}/p0.parquet')
