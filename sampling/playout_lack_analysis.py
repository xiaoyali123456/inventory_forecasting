from pyspark.shell import spark
from pyspark.sql import SparkSession, DataFrame
from pyspark.storagelevel import StorageLevel
import pyspark.sql.functions as F
from pyspark.sql.types import *
import pandas as pd


PLAYOUT_PATH = 's3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test/'
INVENTORY_SAMPLING_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_v2/inventory/'


@F.udf(returnType=TimestampType())
def parse_timestamp(date: str, ts: str):
    return pd.Timestamp(date + ' ' + ts, tz='asia/kolkata')


def preprocess_playout(df):
    return df\
        .withColumn('break_start', parse_timestamp('Start Date', 'Start Time')) \
        .withColumn('break_start', parse_timestamp('Start Date', 'Start Time')) \
        .withColumn('break_end', parse_timestamp('End Date', 'End Time')) \
        .selectExpr(
            '`Content ID` as content_id',
            'trim(lower(`Playout ID`)) as playout_id',
            'trim(lower(Language)) as language',
            'trim(lower(Tenant)) as country',
            'explode(split(trim(lower(Platform)), "\\\\|")) as platform',
            'break_start',
            'break_end',
        )\
        .where('break_start is not null and break_end is not null') \
        .withColumn('break_start', F.expr('cast(break_start as long)')) \
        .withColumn('break_end', F.expr('cast(break_end as long)'))


result_df = spark.read.parquet(INVENTORY_SAMPLING_PATH).where('cd >= "2023-01-01"').cache()
date_list = result_df.select('cd').orderBy('cd').distinct().collect()
for date_tmp in date_list:
    date = str(date_tmp[0])
    print(date)
    content_ids = [content_id[0] for content_id in result_df.where(f'cd="{date}"').select('content_id').distinct().collect()]
    raw_playout = spark.read.csv(PLAYOUT_PATH + date, header=True)
    # print("preprocess done!")
    raw_playout = raw_playout.where(raw_playout['Start Date'].isNotNull() & raw_playout['End Date'].isNotNull()) # TODO: this doesn't cover all corner cases
    # print("preprocess done!")
    playout = preprocess_playout(raw_playout).where(F.col('content_id').isin(content_ids))
    # playout = raw_playout.where(F.col('content_id').isin(content_ids))
    # print("preprocess done!")
    playout.toPandas() # try to realize the playout
    print(playout.where('platform == "na"').count(), playout.count(), playout.where('platform == "na"').count()/playout.count())


