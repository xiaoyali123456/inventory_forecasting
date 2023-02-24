# Custom Audience Analysis
# aws s3 ls s3://hotstar-ads-targeting-us-east-1-prod/adw/user-segment/custom-audience/ > CA.txt
# 2022-11-23 -> 2023-02-22 (only not continous)
# aws s3 ls s3://hotstar-ads-targeting-us-east-1-prod/adw/user-segment/ap_user_tag/ > CA_ap.txt
# 2022-11-24 -> 2023-02-23 (92 days, continous)
import pandas as pd
import pyspark.sql.functions as F

out = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/CA_exp_cnt'

# df=spark.read.parquet('s3://hotstar-ads-targeting-us-east-1-prod/adw/user-segment/custom-audience/')
# df2=df.selectExpr('cd', 'to_date(expiry_time) as exp').groupby('cd', 'exp').count()
# df2.write.parquet(f'{out}/type=CA/')

# df = spark.read.parquet('s3://hotstar-ads-targeting-us-east-1-prod/adw/user-segment/ap_user_tag/')
# df2=df.selectExpr('cd', 'to_date(expiry_time) as exp').groupby('cd', 'exp').count()
# df2.write.parquet(f'{out}/type=AP/')

df = spark.read.parquet(out)
df2=df.toPandas()
ca=df2[df2.type == 'CA'].pivot('cd','exp','count').fillna(0)
ap=df2[df2.type == 'AP'].pivot('cd','exp','count').fillna(0)

df2['diff'] = df2.exp-df2.cd
df2[df2.cd < pd.to_datetime('2023-02-23')]

df3=df2.groupby(['type', 'diff'])['count'].sum()

