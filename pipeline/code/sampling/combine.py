import pandas as pd
import sys
from common import *

if __name__ == '__main__':
    DATE = sys.argv[1]
    total = pd.read_parquet(f'{TOTAL_INVENTORY_PREDICTION_PATH}cd={DATE}/')
    reach_ratio = pd.read_parquet(f'{REACH_SAMPLING_PATH}cd={DATE}/')
    ad_time_ratio = pd.read_parquet(f'{AD_TIME_SAMPLING_PATH}cd={DATE}/')
    ad_time_ratio.rename(columns={'ad_time':'inventory'}, inplace=True)
    for i, row in total.iterrows():
        reach = reach_ratio.copy()
        inventory = ad_time_ratio.copy()
        reach.reach *= row.estimated_reach
        inventory.inventory *= row.estimated_inventory
        reach.to_parquet(f'{FINAL_REACH_PREDICTION_PATH}cd={DATE}/p{i}.parquet')
        inventory.to_parquet(f'{FINAL_INVENTORY_PREDICTION_PATH}cd={DATE}/p{i}.parquet')
        common_cols = list(set(reach.columns)&set(inventory.columns))
        combine = inventory.merge(reach, on=common_cols, how='left')
        combine['request_id'] = str(row.request_id)
        combine['match_id'] = str(row.match_id)
        combine.to_parquet(f'{FINAL_ALL_BACKUP_PREDICTION_PATH}cd={DATE}/p{i}.parquet')
        # reformat
        combine['adPlacement'] = 'MIDROLL'
        combine.inventory = combine.inventory.astype(int)
        combine.reach = combine.reach.astype(int)
        combine['version'] = 'v1'
        combine = combine[(combine.inventory >= 1)&(combine.reach >= 1)].reset_index()
        combine.to_parquet(f'{FINAL_ALL_PREDICTION_PATH}cd={DATE}/p{i}.parquet')
