"""
    1.
"""
import pandas as pd
import sys
import json
from common import *


def parse(string):
    if string is None or string == '':
        return False
    lst = [x.lower() for x in json.loads(string)]
    return lst


if __name__ == '__main__':
    DATE = sys.argv[1]
    total = spark.read_parquet(f'{TOTAL_INVENTORY_PREDICTION_PATH}cd={DATE}/').toPandas()
    reach_ratio = pd.read_parquet(f'{REACH_SAMPLING_PATH}cd={DATE}/')
    ad_time_ratio = pd.read_parquet(f'{AD_TIME_SAMPLING_PATH}cd={DATE}/')
    ad_time_ratio.rename(columns={'ad_time': 'inventory'}, inplace=True)
    processed_input = pd.read_parquet(PREPROCESSED_INPUT_PATH + f'cd={DATE}/')
    for i, row in total.iterrows():
        reach = reach_ratio.copy()
        inventory = ad_time_ratio.copy()
        reach.reach *= row.estimated_reach
        inventory.inventory *= row.estimated_inventory
        common_cols = list(set(reach.columns) & set(inventory.columns))
        combine = inventory.merge(reach, on=common_cols, how='left')
        row.request_id = str(row.request_id)
        row.match_id = int(row.match_id)
        combine['request_id'] = row.request_id
        combine['matchId'] = row.match_id
        # TODO: fix the below; currently we have category type ID
        # meta_info = processed_input[(processed_input.requestId == row.request_id)&(processed_input.matchId == row.match_id)]
        meta_info = processed_input[(processed_input.matchId == row.match_id)].iloc[0]
        combine['tournamentId'] = meta_info['tournamentId']
        combine['seasonId'] = meta_info['seasonId']
        # reformat
        combine['adPlacement'] = 'MIDROLL'
        # TODO: fix the below with scaling up factor
        languages = parse(meta_info.contentLanguages)
        if languages:
            combine.reach *= combine.reach.sum()/combine[combine.language.isin(languages)].reach.sum()
            combine.inventory *= combine.inventory.sum()/combine[combine.language.isin(languages)].inventory.sum()
            combine = combine[combine.language.isin(languages)].reset_index(drop=True)
        platforms = parse(meta_info.platformsSupported)
        if platforms:
            combine.reach *= combine.reach.sum()/combine[combine.platform.isin(platforms)].reach.sum()
            combine.inventory *= combine.inventory.sum()/combine[combine.platform.isin(platforms)].inventory.sum()
            combine = combine[combine.platform.isin(platforms)].reset_index(drop=True)
        combine.inventory = combine.inventory.astype(int)
        combine.reach = combine.reach.astype(int)
        # TODO: the below code should be unnecessary
        combine.replace({'device': {'15-20K': 'A_15031263', '20-25K': 'A_94523754', '25-35K': 'A_40990869', '35K+': 'A_21231588'}}, inplace=True)
        combine = combine.rename(columns={
            'age': 'ageBucket',
            'device': 'devicePrice',
            'request_id': 'inventoryId',
            'custom_cohorts': 'customCohort',
        })
        combine = combine[(combine.inventory >= 1)
                          &(combine.reach >= 1)
                          &(combine.city.map(len)!=1)].reset_index(drop=True)
        combine.to_parquet(f'{FINAL_ALL_PREDICTION_PATH}cd={DATE}/p{i}.parquet')
    df = spark.read.parquet(f'{FINAL_ALL_PREDICTION_PATH}cd={DATE}/')
    df.write.partitionBy('tournamentId').parquet(f'{FINAL_ALL_PREDICTION_TOURNAMENT_PARTITION_PATH}cd={DATE}/')
