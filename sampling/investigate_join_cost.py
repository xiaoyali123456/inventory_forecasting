# investigate join cost
import pyspark.sql.functions as F

df=spark.read.parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/sampling/cohort_agg_cache_wt3/part-00000-df097c03-6b57-45f3-a9c8-498f2a1c67ca-c000.snappy.parquet')
df.show()
print(df.count())
df2 = df.withColumn('break_start', F.expr('explode(break_start)'))
print(df2.count())
df2.write.parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/sampling/testdata/')

df.drop('break_start','break_end').write.parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/sampling/testdata2/')

df=spark.read.parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/sampling/cohort_agg_cache_wt/part-00000-e9ac052e-d662-4be9-8d59-52a60ccaf03c-c000.snappy.parquet')
df.selectExpr('timestamp-make_interval(0,0,0,0,0,0,watch_time)', 'timestamp', 'watch_time').show()
df.selectExpr('least(0, float(make_interval(0,0,0,0,0,0,watch_time)))', 'watch_time').show()
df.selectExpr('bigint(timestamp-timestamp)') # work
df.selectExpr('make_interval(0,0,0,0,0,0,watch_time)', 'watch_time') # not work
