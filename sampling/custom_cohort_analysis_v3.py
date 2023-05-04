import json
import os
from datetime import datetime
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
import s3fs

spark = SparkSession.builder \
    .config("spark.sql.hive.convertMetastoreParquet", "false") \
    .config("hive.metastore.uris", "thrift://metastore.data.hotstar-labs.com:9083") \
    .enableHiveSupport() \
    .getOrCreate()

s3 = s3fs.S3FileSystem()
c_tags = ['AP_2AS', 'AP_107', 'AP_2AT', 'AP_3YF', 'AP_3YC']

t = s3.glob('hotstar-ads-targeting-us-east-1-prod/adw/user-segment/ap_user_tag/cd*/hr*/segment*/')
t2 = s3.glob('hotstar-ads-targeting-us-east-1-prod/adw/user-segment/custom-audience/cd*/hr*/segment*/')

def f(s):
    for c in c_tags:
        if s.endswith(c):
            return True
    return False

t3 = [x for x in t if f(x)] + [x for x in t2 if f(x)]




