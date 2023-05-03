import pandas as pd
import numpy as np
import pyspark.sql.functions as F
from pyspark.sql.window import Window

a = pd.DataFrame({'k': list('abc')})
b = pd.DataFrame({
    'i': np.arange(5), 
    'v': np.random.randint(100, size=5)
})
df = a.merge(b, how='cross')
df2 = df.pivot_table(index='i', columns='k', values='v')

# Pandas method
lambda_ = 0.8
fun = np.frompyfunc(lambda x,y: lambda_ * x + (1-lambda_) * y, 2, 1) # x is the sum
df3 = pd.concat([fun.accumulate(df2[x], dtype=object) for x in df2.columns], axis=1).shift(1)
df4 = df2.ewm(alpha=1-lambda_, adjust=False).mean()

# Spark method: TODO: messy up
sdf = spark.createDataFrame(df)
sdf2 = sdf.withColumn('v', F.col('v') * F.lit(1-lambda_) * F.pow(
        F.lit(lambda_),
        F.expr('row_number() over (partition by k order by i)-1')
    )
)
sdf[[F.expr('row_number() over (partition by k order by i)-1')]].show()

sdf2.groupby('k').agg(F.sum('v')).show()
