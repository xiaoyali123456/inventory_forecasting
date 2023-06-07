import pandas as pd
import sys
from common import *

if __name__ == '__main__':
    DATE = sys.argv[1]
    total = pd.read_parquet(f'{TOTAL_INVENTORY_PREDICTION_PATH}cd={DATE}/')
    ad_time_ratio = pd.read_parquet(f'{AD_TIME_SAMPLING_PATH}cd={DATE}/')
    reach_ratio = pd.read_parquet(f'{REACH_SAMPLING_PATH}cd={DATE}/')
    ad_time_ratio.ad_time *= total.estimated_inventory.iloc[0]
    ad_time_ratio.rename(columns={'ad_time':'inventory'}, inplace=True)
    reach_ratio.reach *= total.estimated_reach.iloc[0]
    ad_time_ratio.to_parquet(f'{FINAL_INVENTORY_PREDICTION_PATH}cd={DATE}/p0.parquet')
    reach_ratio.to_parquet(f'{FINAL_REACH_PREDICTION_PATH}cd={DATE}/p0.parquet')
    common_cols = list(set(reach_ratio.columns)&set(ad_time_ratio.columns))
    ad_time_ratio.merge(reach_ratio, on=common_cols, how='left').to_parquet(f'{FINAL_ALL_PREDICTION_PATH}cd={DATE}/p0.parquet')
