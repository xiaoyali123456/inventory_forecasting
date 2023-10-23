# common api definition

# import datetime
from datetime import datetime, timedelta
import os
import s3fs
import json

from pyspark.shell import spark
from pyspark.sql import SparkSession, DataFrame
from pyspark.storagelevel import StorageLevel
import pyspark.sql.functions as F


storageLevel = StorageLevel.DISK_ONLY
spark.sparkContext.setLogLevel('WARN')
s3 = s3fs.S3FileSystem(use_cache=False)


def check_s3_path_exist(s3_path: str) -> bool:
    if not s3_path.endswith("/"):
        s3_path += "/"
    return os.system(f"aws s3 ls {s3_path}_SUCCESS") == 0


def check_s3_folder_exist(s3_path: str) -> bool:
    if not s3_path.endswith("/"):
        s3_path += "/"
    return os.system(f"aws s3 ls {s3_path}") == 0


def get_date_list(date: str, days: int) -> list:
    dt = datetime.strptime(date, '%Y-%m-%d')
    if -1 <= days <= 1:
        return [date]
    elif days > 1:
        return [(dt + timedelta(days=n)).strftime('%Y-%m-%d') for n in range(0, days)]
    else:
        return [(dt + timedelta(days=n)).strftime('%Y-%m-%d') for n in range(days + 1, 1)]


def load_data_frame(spark: SparkSession, path: str, fmt: str = 'parquet', header: bool = False, delimiter: str = ','
                    ) -> DataFrame:
    if fmt == 'parquet':
        return spark.read.option("mergeSchema", "true").parquet(path)
    elif fmt == 'orc':
        return spark.read.option("mergeSchema", "true").orc(path)
    elif fmt == 'jsond':
        return spark.read.option("mergeSchema", "true").json(path)
    elif fmt == 'csv':
        return spark.read.option("mergeSchema", "true").option('header', header).option('delimiter', delimiter).csv(path)
    else:
        print("the format is not supported")
        return DataFrame(None, None)


def get_s3_paths(bucket_name, folder_path):
    import boto3
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket_name)
    file_list = []
    for object_summary in my_bucket.objects.filter(Prefix=folder_path):
        if object_summary.key.endswith("csv"):
            file_list.append(f"s3://{bucket_name}/" + object_summary.key)
    return file_list


def load_multiple_csv_file(spark, file_paths, delimiter: str = ','):
    # Initialize an empty list to store all the columns
    all_columns = []
    # Iterate over the file paths to get all the columns
    for file_path in file_paths:
        # Read the CSV file
        # print(file_path)
        df = spark.read.option("header", "true").csv(file_path)
        # Get the columns of the current DataFrame
        columns = df.columns
        # print(file_path)
        # print(columns)
        # Add the columns to the list of all columns
        all_columns.extend(columns)
    # Remove duplicate columns
    all_columns = list(set(all_columns))
    # print(all_columns)
    all_columns.remove('Sr. No.')
    # Initialize an empty DataFrame with the desired schema
    output_df = spark.read.option("header", "true").option('delimiter', delimiter).csv(file_paths[0])
    # output_df.printSchema()
    missing_cols = set(all_columns) - set(output_df.columns)
    for col in missing_cols:
        output_df = output_df.withColumn(col, F.lit(None).cast("string"))
    output_df = output_df.select(*all_columns)
    # Iterate over the file paths again to merge the data
    for file_path in file_paths[1:]:
        # Read the CSV file
        df = spark.read.option("header", "true").option('delimiter', delimiter).csv(file_path)
        # Add missing columns to the current DataFrame
        missing_cols = set(all_columns) - set(df.columns)
        for col in missing_cols:
            df = df.withColumn(col, F.lit(None).cast("string"))
        # Select the desired columns in the desired order
        df = df.select(*all_columns)
        # Union the current DataFrame with the output DataFrame
        output_df = output_df.union(df)
    # Show the final DataFrame
    return output_df


def hive_spark(name: str) -> SparkSession:
    return SparkSession.builder \
        .appName(name) \
        .config("spark.sql.hive.convertMetastoreParquet", "false") \
        .config("hive.metastore.uris", "thrift://metastore.data.hotstar-labs.com:9083") \
        .config("spark.kryoserializer.buffer.max", "128m") \
        .enableHiveSupport() \
        .getOrCreate()


def load_hive_table(spark: SparkSession, table: str, date: str = None) -> DataFrame:
    if date is None:
        return spark.sql(f'select * from {table}')
    else:
        return spark.sql(f'select * from {table} where cd = "{date}"')


def save_data_frame(df: DataFrame, path: str, fmt: str = 'parquet', header: bool = False, delimiter: str = ',', partition_col = '') -> None:
    def save_data_frame_internal(df: DataFrame, path: str, fmt: str = 'parquet', header: bool = False,
                                 delimiter: str = ',') -> None:
        if fmt == 'parquet':
            if partition_col == "":
                df.write.mode('overwrite').parquet(path)
            else:
                df.write.partitionBy(partition_col).mode('overwrite').parquet(path)
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


def slack_notification(topic, region, message):
    cmd = f'aws sns publish --topic-arn "{topic}" --subject "midroll inventory forecasting" --message "{message}" --region {region}'
    os.system(cmd)


def load_requests(cd, request_path):
    with s3.open(request_path % cd) as fp:
        return json.load(fp)


# end is exclusive
def get_last_cd(path, end=None, n=1, invalid_cd=None):
    lst = [x.split('=')[-1] for x in s3.ls(path)]
    lst = sorted([x for x in lst if '$' not in x])
    if end is not None:
        lst = [x for x in lst if x < end]
    if invalid_cd is not None and invalid_cd in lst:
        lst.remove(invalid_cd)
    if n > 1:
        return lst[-n:]
    else:
        if len(lst) > 0:
            return lst[-1]
        return None


def update_single_dashboard(spark, table):
    spark.sql(f"msck repair table {table}")


def update_dashboards():
    # spark.stop()
    spark = hive_spark("update_dashboards")
    table_list = ['adtech.daily_vv_report', 'adtech.daily_predicted_vv_report',
                  'adtech.daily_predicted_inventory_report',
                  'adtech.daily_midroll_corhort_report', 'adtech.daily_midroll_corhort_final_report',
                  'adtech.daily_inventory_forecasting_metrics']
    for table in table_list:
        update_single_dashboard(spark, table)


def get_df_str(input_df):
    return input_df._jdf.showString(20, 0, False)


def publish_to_slack(topic, title, output_df, region):
    cmd = 'aws sns publish --topic-arn "{}" --subject "{} report" --message "{}" --region {}'. \
        format(topic, title, get_df_str(output_df), region)
    os.system(cmd)

