import pandas as pd
import datetime
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.shell import spark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
storageLevel = StorageLevel.DISK_ONLY


def save_data_frame(df: DataFrame, path: str, fmt: str = 'parquet', header: bool = False, delimiter: str = ',') -> None:
    def save_data_frame_internal(df: DataFrame, path: str, fmt: str = 'parquet', header: bool = False,
                                 delimiter: str = ',') -> None:
        if fmt == 'parquet':
            df.write.mode('overwrite').parquet(path)
        elif fmt == 'parquet2':
            df.write.mode('overwrite').parquet(path, compression='gzip')
        elif fmt == 'parquet3':
            df.write.mode('overwrite').parquet(path, compression='uncompressed')
        elif fmt == 'orc':
            df.write.mode('overwrite').orc(path)
        elif fmt == 'csv':
            df.coalesce(1).write.option('header', header).option('delimiter', delimiter).mode('overwrite').csv(path)
        elif fmt == 'csv_zip':
            df.write.option('header', header).option('delimiter', delimiter).option("compression", "gzip").mode(
                'overwrite').csv(path)
        else:
            print("the format is not supported")
    df.persist(storageLevel)
    try:
        save_data_frame_internal(df, path, fmt, header, delimiter)
    except Exception:
        try:
            save_data_frame_internal(df, path, 'parquet2', header, delimiter)
        except Exception:
            save_data_frame_internal(df, path, 'parquet3', header, delimiter)
    df.unpersist()


header='Sr. No.,Start Date,Start Time,Playout ID,Content ID,Language,Tenant,Stream Type,Platform,Creative ID,Break ID,Creative Path,End Date,End Time,Actual Time,Delivered Time,cd'.split(',')
neo = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/playout_v2/'

df = spark.read.csv("playout_v2/", header=True).cache()
res_df = df\
    .withColumnRenamed('Start Time', 'Start_Time')\
    .where('Start_Time is not null')\
    .withColumnRenamed('Start Time', 'Start_Time')\
    .cache()
valid_dates = res_df.select('cd').orderBy('cd').distinct().collect()
for date in valid_dates:
    date_str = str(date[0])
    print(date_str)
    save_data_frame(res_df.where(f'cd="{date_str}"'), f"{neo}cd={date_str}", "csv", True)


df = spark.read.csv(neo, header=True).where('cd < "2021-12-31"').toPandas()
df = spark.read.csv(neo, header=True).where('cd < "2021-12-31"').cache()
assert list(df.columns) == header


def to_date_safe(dt):
    try:
        return pd.to_datetime(dt)
    except:
        return None


df['start'] = (df.cd.map(str) + ' ' + df['Start Time']).map(to_date_safe)
df['end'] = (df.cd.map(str) + ' ' + df['End Time']).map(to_date_safe)

print(df[df.start.isna()])  #[['Start Date', 'Start Time', 'cd', 'Content ID']])
print(df[df.end.isna()])  #[['End Date', 'End Time', 'cd', 'Content ID']])
df[~df.start.isna()].count()
## check malform

x = df[~df.start.isna()].start.map(lambda t: t.time())
print(df[~df.start.isna()][x.apply(lambda t: t < datetime.time(5, 30))].cd.drop_duplicates())
# problematic date:
#   2022-11-06
#         hr ad_break_count
#         4      28
#         5     299
#         6     727
#         7     647
#         8     554
#         9     422
#         10    685
#         11    589
#         12    601
#         13    439
#         14    738
#         15    712
#         16    621
#         17     42
#   2021-11-14: no problem
#         18    352
#         19    583
#         20    793
#         21    664
#         22    507
#         23     81
#         0       2

# print(x[df[~df.start.isna()].cd == pd.to_datetime('2022-11-06')].apply(lambda x: x.hour).value_counts().sort_index())
# print(x[df[~df.start.isna()].cd == pd.to_datetime('2021-11-14')].apply(lambda x: x.hour).value_counts().sort_index())

# check End date
y = df[~df.end.isna()].end.map(lambda t: t.time())
print(df[~df.end.isna()][y.apply(lambda t: t < datetime.time(5, 30))].cd.drop_duplicates())
