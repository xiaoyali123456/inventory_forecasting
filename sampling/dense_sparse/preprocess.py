import json
import pandas as pd
import os

import pyspark.sql.functions as F
from pyspark.sql.types import StringType

output_root = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/dense_sparse/qdata/'
watched_video_path = 's3://hotstar-ads-ml-us-east-1-prod/data_exploration/data/data_backup/watched_video/'
dates = pd.read_json('dates.json')

@F.udf(returnType=StringType())
def parse(segments):
    try:
        js = json.loads(segments)
        if type(js) == list:
            lst = js
        else:
            lst =js.get('data', [])
    except:
        return None
    filtered = set()
    equals = ['A_15031263', 'A_94523754', 'A_40990869', 'A_21231588', # device price
              'A_34365936', 'A_49094287', 'AP_107', 'AP_2AS', 'AP_2AT'] # sponsor custom cohort
    prefixs = ['NCCS_', 'FMD00', 'MMD00', 'CITY_', 'STATE_']
    middles = ['_MALE_', '_FEMALE_']
    for t in lst:
        match = False
        if t.startswith('SSAI::'):
            for s in t.split(':'):
                if s.startswith('C'):
                    filtered.add(s)
        if not match:
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


def process(dt, tour):
    print('process', dt)
    print('begin', pd.datetime.now())
    npar = 32
    out_table_path = f'{output_root}quarter_data/tournament={tour}/cd={dt}/'
    success_path = f'{out_table_path}_SUCCESS'
    if os.system('aws s3 ls ' + success_path) == 0:
        return
    wt = spark.read.parquet(watched_video_path + 'cd=' + dt)
    wt1 = wt[['dw_d_id',
        F.expr('lower(genre) == "cricket" as is_cricket'),
        F.expr('lower(language) as language'),
        F.expr('lower(platform) as platform'),
        F.expr('lower(country) as country'),
        F.expr('lower(city) as city'),
        F.expr('lower(state) as state'),
        F.expr('case when watch_time < 86400 then watch_time else 0 end as watch_time'),
        parse('user_segments').alias('segments'),
    ]]
    basic = ['is_cricket', 'language', 'platform', 'country', 'city', 'state', 'segments']
    wt2 = wt1.groupby(*basic).agg(
            F.expr('sum(watch_time) as watch_time'),
            F.expr('count(distinct dw_d_id) as reach')
        ).repartition(npar)
    wt2.write.mode('overwrite').parquet(out_table_path)
    print('end', pd.datetime.now())


df = spark.read.parquet(watched_video_path + 'cd=2022-10-16')
df.

