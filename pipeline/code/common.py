import s3fs
import json

# config
REQUESTS_PATH_TEMPL = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/inventory_requests/cd=%s/requests.json'
NEW_MATCHES_PATH_TEMPL = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/cms_match/cd=%s/'
SERVER_URL_ROOT = 'http://localhost:4321/'

s3 = s3fs.S3FileSystem()
def load_requests(cd):
    with s3.open(REQUESTS_PATH_TEMPL % cd) as fp:
        return json.load(fp)

# importing Spark may fail on pure python application
try:
    import pyspark.sql.functions as F
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StringType
    spark = SparkSession.builder \
        .config("spark.sql.hive.convertMetastoreParquet", "false") \
        .config("hive.metastore.uris", "thrift://metastore.data.hotstar-labs.com:9083") \
        .enableHiveSupport() \
        .getOrCreate()
except ImportError:
     pass

tournaments = [
    "australia tour of india",
    "ipl",
    "cricket world cup",
    "sri lanka tour of pakistan",
    "west indies tour india",
    "india tour of new zealand",
    "england tour of india",
    "world cup",
    "west indies tour of india",
    "south africa tour of india",
    "asia cup",
    "sri lanka tour of india",
    "new zealand tour of india",
]