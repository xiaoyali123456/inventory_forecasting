# playwith spark IST
import pyspark.sql.functions as F

df = spark.createDataFrame([('1970-01-02 00:00:00', 'IST')], ['ts', 'tz'])
df.withColumn('ts2', F.from_utc_timestamp(F.col('ts'), "IST")).select(
    F.expr('cast(unix_timestamp(ts, "yyyy-MM-dd HH:mm:ss") as long)'),
    F.expr('cast(unix_timestamp(ts2, "yyyy-MM-dd HH:mm:ss") as long)')
).show()