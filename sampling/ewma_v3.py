import pandas as pd
import numpy as np
from tqdm import tqdm

time_cols = ['cd']
lambda_ = 0.8

df = pd.read_parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/tmp/piv.parquet')
df2 = pd.read_parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/tmp/df2.parquet')


