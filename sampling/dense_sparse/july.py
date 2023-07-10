import pandas as pd
spark = SparkSession.builder \
        .config("spark.sql.hive.convertMetastoreParquet", "false") \
        .config("hive.metastore.uris", "thrift://metastore.data.hotstar-labs.com:9083") \
        .enableHiveSupport() \
        .getOrCreate()
sc.setLogLevel("ERROR")

df = pd.read_parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/dense_sparse/qdata_v4/')
df2 = df[~(df.is_cricket==False)].reset_index()

cd = '2023-06-11'
cid = '1540023557'
gt = spark.sql(f'select * from concurrency.users_by_live_sports_content_by_ssai where cd = "{cd}" and content_id = "{cid}"').toPandas()

cls = pd.read_csv('cohort_categories_2023_06_08.csv').rename(columns={'cohort': 'ssai_tag'})
gt2 = gt.merge(cls, on='ssai_tag', how='left')

err = gt2[gt2.category.isna()]
print(err.ssai_tag.iloc[:5].tolist())
print(df2.columns)

# first case
df2[df2.age=='18T30'][(df2.gender=='other')|df2.gender.isna()]

# [df2.state=='tg'][df2.city.isin(['medchal'])][df2.platform.isin(['androidtv', 'android_tv'])]# [df2.nccs][df2.device]
# second case
err[~err.ssai_tag.map(lambda s: ('G_U' in s) and ('18T30' in s))].ssai_tag
err[~err.ssai_tag.map(lambda s: ('G_U' in s))].ssai_tag

print(err.no_user.map(float).sum())
print(err[~err.ssai_tag.map(lambda s: ('G_U' in s))].no_user.map(float).sum())
print(gt.no_user.map(float).sum())
