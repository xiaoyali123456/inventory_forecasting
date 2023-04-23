import s3fs
import json

# preprocess
REQUESTS_PATH_TEMPL = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/inventory_requests/cd=%s/requests.json'
NEW_MATCHES_PATH_TEMPL = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/cms_match/cd=%s/'
BOOKING_TOOL_URL = 'http://localhost:4321/'

# DAU
DAU_TRUTH_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_v3/truth/'
DAU_FORECAST_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_v3/forecast/'
DAU_TABLE = 'data_warehouse.watched_video_daily_aggregates_ist'
HOLIDAYS_FEATURE_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/holidays/latest/holidays_v2_4.csv'

s3 = s3fs.S3FileSystem()
def load_requests(cd):
    with s3.open(REQUESTS_PATH_TEMPL % cd) as fp:
        return json.load(fp)

FOCAL_TOURNAMENTS = [
    "ipl",
    "world cup",
    "asia cup",
    "cricket world cup",
    # "sri lanka tour of pakistan",
    # "west indies tour india",
    # "india tour of new zealand",
    # "england tour of india",
    # "west indies tour of india",
    # "south africa tour of india",
    # "sri lanka tour of india",
    # "new zealand tour of india",
    # "australia tour of india",
]

# importing will fail on pure python application
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
