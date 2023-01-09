import json
import pandas as pd
import pyspark.sql.functions as F

wt_root = 's3://hotstar-dp-datalake-processed-us-east-1-prod/events/watched_video/'
# aws s3 sync
tour = 'wc2022.json'
with open(tour) as f:
    dates = json.load(f)
dates = []
