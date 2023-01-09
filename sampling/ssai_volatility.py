import json
import pandas as pd
import pyspark.sql.functions as F

wt_root = 's3://hotstar-dp-datalake-processed-us-east-1-prod/events/watched_video/'
