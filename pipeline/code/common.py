import s3fs
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

s3 = s3fs.S3FileSystem()
spark = SparkSession.builder \
        .config("spark.sql.hive.convertMetastoreParquet", "false") \
        .config("hive.metastore.uris", "thrift://metastore.data.hotstar-labs.com:9083") \
        .enableHiveSupport() \
        .getOrCreate()
