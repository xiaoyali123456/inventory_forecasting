import pandas as pd

spark = SparkSession.builder \
        .config("spark.sql.hive.convertMetastoreParquet", "false") \
        .config("hive.metastore.uris", "thrift://metastore.data.hotstar-labs.com:9083") \
        .enableHiveSupport() \
        .getOrCreate()
sc.setLogLevel("ERROR")

cd = '2023-06-11'
cid = '1540023557'
wt = spark.sql(f'select dw_d_id, user_segment from data_lake.watched_video where cd = "{cd}" and content_id = "{cid}"')

print('count', wt.count()) # 154735764
print('seg count', wt.where('user_segment is not null').count()) # 146340583
print(wt.where('user_segment is not null and user_segment != ""').head(10)) # [], no result

# iv = spark.sql(f'select dw_d_id, user_segment, server_timestamp from adtech.shifu_inventory where cd = "{cd}" and content_id = "{cid}"')
# print(iv.count())

im = spark.sql(f'select * from concurrency.users_by_live_sports_content_by_ssai where cd = "{cd}" and content_id = "{cid}"')
df = pd.read_csv('cohort_categories_2023_06_08.csv')
df['ssai_tag'] = df['cohort']
df2 = spark.createDataFrame(df)
im2 = im.join(df2, on='ssai_tag', how='left')
cnt = im2.selectExpr('category', 'cast(no_user as float) as num').groupby('category').sum('num').toPandas()
cnt['rate'] = cnt['sum(num)']/cnt['sum(num)'].sum()
print(cnt)

'''
      category    sum(num)      rate
0         None  24041488.0  0.469305
1  SUPER_DENSE   9040021.0  0.176467
2       SPARSE    717931.0  0.014014
3        DENSE  17428440.0  0.340214
'''

im2.where('category is null and ssai_tag !=""').show(10, False)


