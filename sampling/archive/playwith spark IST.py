# playwith spark IST
import pyspark.sql.functions as F

df = spark.createDataFrame([('1970-01-02 00:00:00', 'IST')], ['ts', 'tz'])
df.withColumn('ts2', F.from_utc_timestamp(F.col('ts'), "IST")) \
  .withColumn('ts3', F.to_utc_timestamp(F.col('ts'), "IST")) \
  .withColumn('ts4', F.to_timestamp('ts')) \
  .select(
    'ts', 'ts2', 'ts3', 'ts4',
    F.expr('cast(unix_timestamp(ts, "yyyy-MM-dd HH:mm:ss") as long) as s1'),
    F.expr('cast(unix_timestamp(ts2, "yyyy-MM-dd HH:mm:ss") as long) as s2'),
    F.expr('cast(ts3 as long) as s3'),
    F.expr('cast(ts4 as long) as s4'),
).show()