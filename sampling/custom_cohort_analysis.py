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

# Volume investigation
df = spark.read.parquet(out)
df2 = df.toPandas()
ca = df2[df2.type == 'CA'].pivot('cd','exp','count').fillna(0)
ap = df2[df2.type == 'AP'].pivot('cd','exp','count').fillna(0)

df2['diff'] = df2.exp-df2.cd
df2[df2.cd < pd.to_datetime('2023-02-23')]
df3 = df2.groupby(['type', 'diff'])['count'].sum()

# Tag count
df = spark.read.parquet('s3://hotstar-ads-targeting-us-east-1-prod/adw/user-segment/custom-audience/')
df2 = spark.read.parquet('s3://hotstar-ads-targeting-us-east-1-prod/adw/user-segment/ap_user_tag/')
x=df.selectExpr('cd', 'to_date(expiry_time) as exp', 'segment').where("exp > '2023-02-25' ").distinct().toPandas() #292
y=df2.selectExpr('cd', 'to_date(expiry_time) as exp', 'segment').where("exp > '2023-02-25' ").distinct().toPandas()

x.to_csv(out+'_CA_cnt.csv', index=False)
y.to_csv(out+'_AP_cnt.csv', index=False)

# verify
x=spark.read.csv(out+'_CA_cnt.csv',header=True).toPandas()
y=spark.read.csv(out+'_AP_cnt.csv',header=True).toPandas()


