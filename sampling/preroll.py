import pandas as pd

iv = spark.read.parquet(
    f's3://hotstar-ads-targeting-us-east-1-prod/trackers/shifu_reporting/aggregates/hourly/ad_inventory/'
)


