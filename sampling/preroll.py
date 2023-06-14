import pandas as pd

NEW_MATCHES_PATH_TEMPL = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/cms_match/cd=%s/'
def latest_n_day_matches(cd, n):
    matches = spark.read.parquet(NEW_MATCHES_PATH_TEMPL % cd)
    df = matches[['startdate', 'content_id']].distinct().toPandas().sort_values('startdate')
    return df[df.startdate < cd].iloc[-n:]


cd = '2023-06-14'
matches = latest_n_day_matches(cd, 10)

def process(cd, content_id):
    df = spark.read.parquet(f's3://hotstar-ads-targeting-us-east-1-prod/trackers/shifu_ad_events/ad_inventory/cd={cd}/')
    

for row in matches.itertuples():
    process(row.startdate, row.content_id)


