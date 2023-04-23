# new match
match = spark.read.parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/cms_match/cd=2023-04-23').toPandas()

df = match[["content_id", "sportsseasonname", "title", "shortsummary", "premium", "monetisable",  "startdate"]]
df[['sportsseasonname', 'shortsummary', 'title']]

FOCAL_TOURNAMENTS = [
    "ipl",
    "world cup",
    "asia cup",
    "cricket world cup",
]

def match_filter(s):
    if isinstance(s, str):
        s = s.lower()
        for t in FOCAL_TOURNAMENTS:
            if t in s:
                return True
    return False

df.sort_values('startdate', inplace=True)
df[df.sportsseasonname.map(match_filter)]

# old inventory
inventory = spark.read.parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_v2//inventory/')
