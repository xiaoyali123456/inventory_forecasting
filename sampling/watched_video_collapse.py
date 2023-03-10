# To fix date format in watched_video
# PYSPARK_DRIVER_PYTHON_OPTS='-m IPython' pyspark --name inv_forecast

import json
import pandas as pd
import os

import pyspark.sql.functions as F
from pyspark.sql.types import StringType

out_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/watched_time_for_collapse/'
watched_video_path = 's3://hotstar-ads-ml-us-east-1-prod/data_exploration/data/data_backup/watched_video/'

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
    prefixs = ['NCCS_', 'FMD00', 'MMD00']
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

def process(dt):
    print('process', dt)
    print('begin', pd.datetime.now())
    npar = 32
    out_table_path = f'{out_path}quarter_data/tournament=wc2022/cd={dt}/'
    success_path = f'{out_table_path}_SUCCESS'
    if os.system('aws s3 ls ' + success_path) == 0:
        return
    wt_path = f'{watched_video_path}cd={dt}/'
    wt = spark.read.parquet(wt_path)
    # debug = True
    debug = False
    if debug:
        wt = spark.read.parquet('s3://hotstar-ads-ml-us-east-1-prod/data_exploration/data/data_backup/watched_video/cd=2022-10-16/part-00163-7313c8ab-82bf-482e-be26-aeedfe9e9478-c000.snappy.parquet')
        out_table_path = f'{out_path}debug_out_table/'
        npar = 1
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
    wt2 = wt1.groupby('is_cricket', 'language', 'platform', 'country', 'city', 'state', 'segments').agg(
            F.expr('sum(watch_time) as watch_time'),
            F.expr('count(distinct dw_d_id) as reach')
        ).repartition(npar)
    wt2.write.mode('overwrite').parquet(out_table_path)
    print('end', pd.datetime.now())
    if debug:
        res = spark.read.parquet(out_table_path)
        res.show()
        print(res.count())

def main():
    for dt in pd.date_range('2022-10-16', '2022-11-13'):
        process(dt.date())
    process('2022-11-15')

if __name__ == '__main__':
    main()
