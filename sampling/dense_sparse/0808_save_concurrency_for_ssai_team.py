import pandas as pd
import pyspark.sql.functions as F

spark = SparkSession.builder \
        .config("spark.sql.hive.convertMetastoreParquet", "false") \
        .config("hive.metastore.uris", "thrift://metastore.data.hotstar-labs.com:9083") \
        .enableHiveSupport() \
        .getOrCreate()
sc.setLogLevel("ERROR")

dic = {
    '1540019065': '2022-11-10',  # WC semi-final India v England
    '1540019068': '2022-11-13',
    '1540023557': '2023-06-11',
    '1540023554': '2023-06-10',
}

for cid, cd in dic.items():
    print(cd)
    cc = spark.read.parquet(f's3://hotstar-dp-datalake-processed-us-east-1-prod/hive_internal_database/concurrency.db/users_by_live_sports_content_by_ssai/cd={cd}/')
    cc2 = cc[(cc.content_id == cid)].groupby('ssai_tag', 'time').agg(F.sum('no_user').alias('no_user')).toPandas()
    cc3 = cc2.pivot_table(index='ssai_tag', columns='time', values='no_user')
    cc3.reset_index().fillna(0).to_csv(f'concurrency/{cid}_{cd}.csv')
    cc5 = cc3.fillna(0).rank(method='first', ascending=False)
    # cc6 = cc5.apply(pd.DataFrame.describe, axis=1)
    # cc6['diff'] =cc6['max']-cc6['min']
    # cc6.describe()
