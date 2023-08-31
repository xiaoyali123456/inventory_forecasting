import pandas as pd

spark = SparkSession.builder \
        .config("spark.sql.hive.convertMetastoreParquet", "false") \
        .config("hive.metastore.uris", "thrift://metastore.data.hotstar-labs.com:9083") \
        .enableHiveSupport() \
        .getOrCreate()
sc.setLogLevel("ERROR")

cd = '2023-08-30'
cid = '1540024245'

# wt = spark.sql(f'select dw_d_id, user_segment from data_lake.watched_video where cd = "{cd}" and content_id = "{cid}"')
# print('count', wt.count())
# print('null seg count', wt.where('user_segment is null or user_segment = ""').count())

# im = spark.sql(f'select * from concurrency.users_by_live_sports_content_by_ssai where cd = "{cd}" and content_id = "{cid}"')
im = spark.read.parquet(f's3://hotstar-dp-datalake-processed-us-east-1-prod/hive_internal_database/concurrency.db/users_by_live_sports_content_by_ssai/cd={cd}')

df = pd.read_csv('cohort_categories_2023_06_08.csv')
df['ssai_tag'] = df['cohort']
df2 = spark.createDataFrame(df)
im2 = im.join(df2, on='ssai_tag', how='left')
cnt = im2.selectExpr('category', 'cast(no_user as float) as num').groupby('category').sum('num').toPandas()
cnt2 = im2.select('ssai_tag', 'category').distinct().groupby('category').count().toPandas()
cnt['rate'] = cnt['sum(num)']/cnt['sum(num)'].sum()
cnt2['rate'] = cnt2['count']/cnt2['count'].sum()
print(cnt)
