import pyspark.sql.functions as F
from pyspark.sql.types import StringType

@F.udf(returnType=StringType())
def classify(reach):
    if reach < 500:
        return 'sparse'
    elif reach < 50 * 1000:
        return 'dense'
    else:
        return 'super'
    
df = spark.read.parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/watched_time_for_collapse/quarter_data_v2_2/')
df2 = df[(df.cd > '2022-06-01') & df.is_cricket].withColumn('sparse', classify('reach'))

attr = ['country', 'language', 'platform', 'city', 'state', 'nccs', 'device', 'gender', 'age']
base = df2[df2.cd < '2022-10-21'].groupby(attr).agg(classify(F.mean('reach')).alias('y_hat'))

future = df2[df2.cd >= '2022-10-21'].groupby(['cd'] + attr).agg(classify(F.sum('reach')).alias('y'))

err = future.join(base, on=attr, how='left')

err2 = err.toPandas()

err2.y_hat.fillna('none', inplace=True)

err3 = err2.groupby(['cd', 'y', 'y_hat']).agg(reach=('reach', 'sum'), num=('reach', 'size')).reset_index()

err3.to_csv('sparse_dense_predict_error.csv')

err3.pivot('cd', columns=['y', 'y_hat'], values=['reach', 'num']).to_csv('sparse_dense_predict_pivot.csv')

