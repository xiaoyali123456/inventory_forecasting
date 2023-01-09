header='Sr. No.,Start Date,Start Time,Playout ID,Content ID,Language,Tenant,Stream Type,Platform,Creative ID,Break ID,Creative Path,End Date,End Time,Actual Time,Delivered Time,cd'.split(',')
neo = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/playout_v2/'

df=spark.read.csv(neo, header=True)
assert df.columns == header

a=df.where(df['Start Date']!=df['cd']).groupby('cd').count().toPandas()
b=df.where(df['End Date']!=df['cd']).groupy('cd').count().toPandas()

# all column of df are string, 
