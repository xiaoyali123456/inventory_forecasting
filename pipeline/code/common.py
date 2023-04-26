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

# sampling
INVENTORY_SAMPLING_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_v2/inventory/'
AD_TIME_SAMPLING_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_v2/ad_time/'
REACH_SAMPLING_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_v2/reach/'
PLAYOUT_PATH = 's3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test/'
WV_S3_BACKUP = 's3://hotstar-ads-ml-us-east-1-prod/data_exploration/data/data_backup/watched_video/'
WV_TABLE = 'data_lake.watched_video'

# total inventory
TOTAL_INVENTORY_PREDICTION_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/pipeline/inventory_prediction/future_tournaments/'
FINAL_INVENTORY_PREDICTION_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/final/inventory/'
FINAL_REACH_PREDICTION_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/final/reach/'

FOCAL_TOURNAMENTS = [
    "ipl",
    "world cup",
    "asia cup",
    "cricket world cup",
    "sri lanka tour of pakistan",
    "west indies tour india",
    "india tour of new zealand",
    "england tour of india",
    "west indies tour of india",
    "south africa tour of india",
    "sri lanka tour of india",
    "new zealand tour of india",
    "australia tour of india",
]

s3 = s3fs.S3FileSystem()
def load_requests(cd):
    with s3.open(REQUESTS_PATH_TEMPL % cd) as fp:
        return json.load(fp)

# end is exclusive
def get_last_cd(path, end=None, n=1):
    # df = spark.read.parquet(path)
    # if end is not None:
    #     df = df.where('cd < "{end}"')
    # return str(df.selectExpr('max(cd) as cd').head().cd)
    lst = sorted([x.split('=')[-1] for x in s3.ls(path)])
    if end is not None:
        lst = [x for x in lst if x < end]
    return lst[-n:] if n > 1 else lst[-1]

# importing will fail on pure python application
try:
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as F
    from pyspark.sql.types import StringType, TimestampType
    spark = SparkSession.builder \
        .config("spark.sql.hive.convertMetastoreParquet", "false") \
        .config("hive.metastore.uris", "thrift://metastore.data.hotstar-labs.com:9083") \
        .enableHiveSupport() \
        .getOrCreate()
except ImportError:
    pass
