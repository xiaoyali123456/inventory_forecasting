import json
import os
from datetime import datetime
import difflib

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

wt_root = 's3://hotstar-dp-datalake-processed-us-east-1-prod/events/watched_video/'
out_root = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/ssai_cnt/'
tour = 'wc2022.json'
with open(tour) as f:
    dates = json.load(f)

@F.udf(returnType=StringType())
def parse(segments):
    try:
        js = json.loads(segments)
        if type(js) == list:
            for s in js:
                if s.startswith('SSAI::'):
                    return s
        elif type(js) == dict:
            return js.get('ssaiTag')
    except:
        pass

def count():
    for dt in dates[-7:]:
        print(dt)
        print('begin', datetime.now())
        wt = spark.read.parquet(f'{wt_root}cd={dt}/').withColumn('ssai', parse('user_segments'))
        wt2 = wt.groupby('dw_d_id').agg(F.countDistinct('ssai')).withColumnRenamed('count(ssai)', 'ssai_cnt')
        wt3 = wt2.groupby('ssai_cnt').count()
        wt3.repartition(1).write.mode('overwrite').parquet(f'{out_root}cd={dt}/')
        print('done ', datetime.now())

def save():
    out2 = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/ssai_volatile/'
    dt = dates[-1]
    print(dt)
    print('begin', datetime.now())
    wt = spark.read.parquet(f'{wt_root}cd={dt}/').withColumn('ssai', parse('user_segments'))
    did = wt.groupby('dw_d_id').agg(F.countDistinct('ssai')).where('`count(ssai)` > 1').select('dw_d_id').distinct()
    wt.join(did, 'dw_d_id')[['dw_d_id', 'user_segments', 'ssai', 'timestamp', 'watch_time']].write.parquet(f'{out2}cd={dt}')
    print('done ', datetime.now())

@F.udf(returnType=StringType())
def diff(ssai_list):
    res = ''
    x = ssai_list[0]
    for y in  ssai_list[1:]:
        res += ','.join(s for s in difflib.ndiff(x.split(':'), y.split(':')) if s[0] in '+-') + '|'
    return res

out2='s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/ssai_volatile/cd=2022-11-13/'
df=spark.read.parquet(out2)

# check diff
df2=df.groupby('dw_d_id').agg(F.collect_set('ssai').alias('ssai')).withColumn('diff', diff('ssai'))
df3=df2.groupby('diff').count().toPandas()
df3.sort_values('count',ascending=False).to_csv('ssai_diff.csv',index=False)

# check distribution of maximum ts
df4=df.groupby('dw_d_id').agg(
    F.expr('max(struct(timestamp, ssai)).ssai as ssai'),
    F.sum('watch_time').alias('wt'),
    F.count('*').alias('cnt')
).groupby('ssai').sum('wt', 'cnt').toPandas()

def select(ssai):
    if not ssai:
        return None
    head = 'SSAI::'
    n = len(head)
    lst = ssai[n:].split(':')
    res = [x for x in lst if x.startswith('M_')]
    return res[0] if len(res) else ''
df4['tag'] = df4.ssai.apply(select)
df4a = df4.groupby('tag').sum()
df4a['cnt ratio'] = df4a['sum(cnt)'] / df4a['sum(cnt)'].sum()
df4a['wt ratio'] = df4a['sum(wt)'] / df4a['sum(wt)'].sum()
print(df4a.to_csv())

# origin distribution
df5=df.groupby('ssai').agg(
    F.sum('watch_time').alias('wt'),
    F.count('*').alias('cnt')
).toPandas()
df5['tag'] = df5.ssai.apply(select)
df5a = df5.groupby('tag').sum()
df5a['cnt ratio'] = df5a['cnt'] / df5a['cnt'].sum()
df5a['wt ratio'] = df5a['wt'] / df5a['wt'].sum()
print(df5a.to_csv())
