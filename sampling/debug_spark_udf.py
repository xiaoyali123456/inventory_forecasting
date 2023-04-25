from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
import os
import pwd
import pandas as pd

@F.udf(returnType=StringType())
def user(x):
    return str(pwd.getpwuid(os.getuid())[0])
    # return os.environ.get('USER') # wrong answer

@F.udf(returnType=StringType())
def test_pandas(x):
    return pd.datetime.now().isoformat()

spark = SparkSession.builder \
    .config("spark.sql.hive.convertMetastoreParquet", "false") \
    .config("hive.metastore.uris", "thrift://metastore.data.hotstar-labs.com:9083") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.createDataFrame([[i] for i in range(10)], schema=['x'])
df[['x', user('x')]].show()

'''
+---+-------+                                                                   
|  x|user(x)|
+---+-------+
|  0|   yarn|
|  1|   yarn|
|  2|   yarn|
|  3|   yarn|
|  4|   yarn|
|  5|   yarn|
|  6|   yarn|
|  7|   yarn|
|  8|   yarn|
|  9|   yarn|
+---+-------+
'''

df[['x', test_pandas('x')]].show()
