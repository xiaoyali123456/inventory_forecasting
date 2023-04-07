import json

import pyspark.sql.functions as F
from pyspark.sql.types import StringType

watched_video_path = 's3://hotstar-ads-ml-us-east-1-prod/data_exploration/data/data_backup/watched_video/'

# check dict type consistency
@F.udf(returnType=StringType())
def check(segments):
    try:
        js = json.loads(segments)
        if type(js) == dict:
            a = js["ssaiTag"]
            for x in js["data"]:
                if x.startswith('SSAI::'):
                    b = x
                break
            if a == b:
                return ''
            return a + ' ' + b
        return None
    except:
        return None

df = spark.read.parquet(watched_video_path + 'cd=2022-10-16')
df2 = df.select(check('user_segments').alias('test')).cache()
print(df2.count(), df2[df2.test.isNull()].count(), df2[df2.test == ''].count()) # this is good, all are the same
df2.unpersist()

# check internal vs. external segment
@F.udf(returnType=StringType())
def check2(segments):
    try:
        js = json.loads(segments)
        if type(js) == list:
            lst = js
        else:
            lst =js.get('data', [])
    except:
        return None
    for x in lst:
        if x.startswith('CITY_'):
            return x.split('_')[1]

df2 = df.select('city', check2('user_segments').alias('test'))
print(df.count(), df2[df2.city != df2.test].count()) # 170841106 7593776
print(df2[df2.city.isNull()].count())
print(df2[df2.test.isNull()].count())

