import numpy as np
import pandas as pd
import pyspark.sql.functions as F

wt_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/inventory_wt/cohort_agg/'
wt_q_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/inventory_wt/cohort_agg_quarter/tournament=wc2019/'
playout_path='s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/playout_v2/'

def select(ssai, prefix):
    if not isinstance(ssai, str):
        return ''
    head = 'SSAI::'
    n = len(head)
    res = [x for x in ssai[n:].split(':') if x.startswith(prefix)]
    return res[0] if len(res) else ''

def parse_ssai(df, prefix='M_'):
    col = 'ad_time'
    df['tag'] = df.cohort.apply(lambda x: select(x, prefix))
    df2=df.groupby(['cd', 'content_id', 'tag']).sum()
    return df2.reset_index().pivot(index=['cd', 'content_id'], columns='tag', values=col+'_ratio').fillna(0)

def calc_ratio(df, col, metric_keys):
    df2 = df.groupby(metric_keys).sum().reset_index()
    df2[col + '_ratio'] = df2[col] / df2.groupby(metric_keys[:-1])[col].transform('sum')
    return df2

if __name__ == "__main__":
    col = 'ad_time'
    metric_keys = ['cd', 'content_id', 'cohort']
    wtq = calc_ratio(spark.read.parquet(wt_q_path).toPandas(), col, metric_keys)
    df2 = parse_ssai(wtq)
    