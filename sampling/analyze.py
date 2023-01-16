import numpy as np
import pandas as pd
import pyspark.sql.functions as F

wt_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/inventory_wt/cohort_agg/'
wt_q_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/inventory_wt/cohort_agg_quarter/'
cc_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/sampling/inventory_concurrency/'
playout_path='s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/playout_v2/'
major_cohort_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/major_cohort/'
match_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/match_df/'

def save_topn(wt, cc, n=20):
    wt_top_ssai = wt.groupby('cohort').sum()[col].nlargest(n).index
    wt2 = wt[wt.cohort.isin(wt_top_ssai)].groupby(['cd', 'cohort']).sum()
    cc2 = cc[cc.cohort.isin(wt_top_ssai)].groupby(['cd', 'cohort']).sum()
    topn = wt2.join(cc2, how='outer', rsuffix='_cc')
    topn.to_csv(f'top{n}.csv')

def corr(x):
    a=x[col+'_ratio']
    b=x[col+'_ratio_cc']
    # a=(a-a.mean())/a.std() # Pearson correlation, doesn't make much difference
    # b=(b-b.mean())/a.std()
    return np.dot(a,b)/np.linalg.norm(a)/np.linalg.norm(b)

def sum_ae(x):
    a=x[col+'_ratio']
    b=x[col+'_ratio_cc']
    c = np.abs(a-b)
    return sum(c)

def max_ae(x):
    a=x[col+'_ratio']
    b=x[col+'_ratio_cc']
    c = np.abs(a-b)
    return max(c)

def metric(df, df2, metric_keys):
    # TODO: should 'outer' on 'cohort', but 'inner' on other keys
    # df3 = pd.merge(df, df2, on=metric_keys, how='inner', suffixes=['','_cc']).fillna(0.0)
    df3 = pd.merge(df, df2, on=metric_keys, how='outer', suffixes=['', '_cc']).fillna(0.0)
    return df3.groupby(metric_keys[:-1]).apply(lambda x:pd.Series({
        'corr': corr(x),
        'max_ae': max_ae(x),
        'sum_ae': sum_ae(x),
    }))

def select(ssai):
    head = 'SSAI::'
    n = len(head)
    res = [x for x in ssai[n:].split(':') if x.startswith('M_')]
    return res[0] if len(res) else ''

def parse_ssai(df):
    df['tag'] = df.cohort.apply(select)
    df2=df.groupby(['cd', 'content_id', 'tag']).sum()
    return df2.reset_index().pivot(index=['cd', 'content_id'], columns='tag', values=col+'_ratio').fillna(0)

def cohort_hist(wt, out='test.png'):
    import matplotlib.pyplot as plt
    plt.close()
    wt.groupby('cohort')[col+'_ratio'].sum().plot(bins=100, kind='hist', logy=True)
    plt.savefig(out)

def calc_ratio(df, col, metric_keys):
    df2 = df.groupby(metric_keys).sum().reset_index()
    df2[col + '_ratio'] = df2[col] / df2.groupby(metric_keys[:-1])[col].transform('sum')
    return df2

if __name__ == "__main__":
    col = 'ad_time'
    match_df = spark.read.parquet(match_path).toPandas()[['content_id', 'title']]
    match_df = match_df[~match_df.title.str.contains('follow on|warm-up')]

    metric_keys = ['cd', 'content_id', 'cohort']
    wt = calc_ratio(spark.read.parquet(wt_path).toPandas(), col, metric_keys)
    wtq = calc_ratio(spark.read.parquet(wt_q_path).toPandas().query('tournament == "wc2022"'), col, metric_keys)
    metric(wt, wtq, metric_keys).to_csv(f'wc_{col}_22-22q.csv')

    metric_keys = ['cd', 'content_id', 'cohort']
    wt0 = spark.read.parquet(wt_path).toPandas().merge(match_df, on='content_id')
    cc0 = spark.read.parquet(cc_path).toPandas().rename(columns={'ssai_tag':'cohort'}) \
        .merge(match_df, on='content_id')
    wt = calc_ratio(wt0, col, metric_keys)
    cc = calc_ratio(cc0, col, metric_keys)
    print(metric(wt, cc, metric_keys).to_csv())
    # parse_ssai(wt).to_csv('wc2022_city_quarter.csv')

# wc 2021
    wt = spark.read.parquet(wt_q_path) \
        .where('cd < "2022-01-01"')
    playout = spark.read.csv(playout_path, header=True).selectExpr(
        'lower(trim(`Content ID`)) as content_id',
        'trim(`Playout ID`) as playout_id',
        'lower(trim(Language)) as language',
        'explode(split(Platform, "\\\\|")) as platform'
    ).withColumn('platform', F.expr('lower(trim(platform))'))
    major = wt.join(playout, on=['content_id', 'playout_id']) \
        .groupby('cd', 'content_id', 'language', 'platform') \
        .agg(
            F.expr('sum(ad_time) as ad_time'),
            F.expr('sum(reach) as reach')
        ).toPandas()

    metric_keys = ['cd', 'content_id', 'language']
    m1 = calc_ratio(major, col, metric_keys)
    print(m1.pivot(index=['cd', 'content_id'], columns='language', values=col+'_ratio')
        .fillna(0).to_csv())
    metric_keys = ['cd', 'content_id', 'platform']
    m2 = calc_ratio(major, col, metric_keys)
    print(m2.pivot(index=['cd', 'content_id'], columns='platform', values=col+'_ratio')
        .fillna(0).to_csv())

# wc 2022
    wt = spark.read.parquet(wt_path) # too big for pandas merge
    playout = spark.read.csv(playout_path, header=True).selectExpr(
        'lower(trim(`Content ID`)) as content_id',
        'trim(`Playout ID`) as playout_id',
        'lower(trim(Language)) as language',
        'explode(split(Platform, "\\\\|")) as platform'
    ).withColumn('platform', F.expr('lower(trim(platform))'))
    wt.join(playout, on=['content_id', 'playout_id']) \
        .groupby('cd', 'content_id', 'language', 'platform') \
        .agg(
            F.expr('sum(ad_time) as ad_time'),
            F.expr('sum(reach) as reach')
        ).write.mode('overwrite').parquet(major_cohort_path)

    major = spark.read.parquet(major_cohort_path).toPandas()
    metric_keys = ['cd', 'content_id', 'language']
    m1 = calc_ratio(major, col, metric_keys).merge(match_df, on='content_id')
    print(m1.pivot(index=['cd', 'content_id', 'title'], columns='language', values=col+'_ratio')
        .fillna(0).to_csv())
    metric_keys = ['cd', 'content_id', 'platform']
    m2 = calc_ratio(major, col, metric_keys).merge(match_df, on='content_id')
    print(m2.pivot(index=['cd', 'content_id', 'title'], columns='platform', values=col+'_ratio')
        .fillna(0).to_csv())
