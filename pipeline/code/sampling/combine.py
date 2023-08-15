"""
    1.calculate inventory&reach for each cohort of each match
    2.need to scale the inventory&reach when the languages or platform of the match is uncompleted
"""
import sys

import pandas as pd

from util import *
from path import *


def copy_data_from_yesterday(cd):
    yesterday = get_date_list(cd, -2)[0]
    os.system(f"aws s3 sync {TOTAL_INVENTORY_PREDICTION_PATH}cd={yesterday}/ {TOTAL_INVENTORY_PREDICTION_PATH}cd={cd}/")
    os.system(f"aws s3 sync {FINAL_ALL_PREDICTION_PATH}cd={yesterday}/ {FINAL_ALL_PREDICTION_PATH}cd={cd}/")


def parse(string):
    if string is None or string == '':
        return False
    lst = [x.lower() for x in json.loads(string)]
    return lst


def combine_inventory_and_sampling(cd):
    model_predictions = spark.read.parquet(f'{TOTAL_INVENTORY_PREDICTION_PATH}cd={cd}/').toPandas()
    reach_ratio = pd.read_parquet(f'{REACH_SAMPLING_PATH}cd={cd}/')
    ad_time_ratio = pd.read_parquet(f'{AD_TIME_SAMPLING_PATH}cd={cd}/')
    ad_time_ratio.rename(columns={'ad_time': 'inventory'}, inplace=True)
    processed_input = pd.read_parquet(PREPROCESSED_INPUT_PATH + f'cd={cd}/')
    # sampling match one by one
    for i, row in model_predictions.iterrows():
        reach = reach_ratio.copy()
        inventory = ad_time_ratio.copy()
        # calculate predicted inventory and reach for each cohort
        reach.reach *= row.estimated_reach
        inventory.inventory *= row.estimated_inventory
        common_cols = list(set(reach.columns) & set(inventory.columns))
        combine = inventory.merge(reach, on=common_cols, how='left')
        # add meta data for each match
        row.request_id = str(row.request_id)
        row.match_id = int(row.match_id)
        combine['request_id'] = row.request_id
        combine['matchId'] = row.match_id
        # We assume that matchId is unique for all requests
        # meta_info = processed_input[(processed_input.requestId == row.request_id)&(processed_input.matchId == row.match_id)]
        meta_info = processed_input[(processed_input.matchId == row.match_id)].iloc[0]
        combine['tournamentId'] = meta_info['tournamentId']
        combine['seasonId'] = meta_info['seasonId']
        combine['adPlacement'] = 'MIDROLL'
        # process cases when languages of this match are incomplete
        languages = parse(meta_info.contentLanguages)
        if languages:
            combine.reach *= combine.reach.sum() / combine[combine.language.isin(languages)].reach.sum()
            combine.inventory *= combine.inventory.sum() / combine[combine.language.isin(languages)].inventory.sum()
            combine = combine[combine.language.isin(languages)].reset_index(drop=True)
        # process case when platforms of this match are incomplete
        platforms = parse(meta_info.platformsSupported)
        if platforms:
            combine.reach *= combine.reach.sum() / combine[combine.platform.isin(platforms)].reach.sum()
            combine.inventory *= combine.inventory.sum() / combine[combine.platform.isin(platforms)].inventory.sum()
            combine = combine[combine.platform.isin(platforms)].reset_index(drop=True)
        combine.inventory = combine.inventory.astype(int)
        combine.reach = combine.reach.astype(int)
        combine.replace(
            {'device': {'15-20K': 'A_15031263', '20-25K': 'A_94523754', '25-35K': 'A_40990869', '35K+': 'A_21231588'}},
            inplace=True)
        combine = combine.rename(columns={
            'age': 'ageBucket',
            'device': 'devicePrice',
            'request_id': 'inventoryId',
            'custom_cohorts': 'customCohort',
        })
        combine = combine[(combine.inventory >= 1)
                          & (combine.reach >= 1)
                          & (combine.city.map(len) != 1)].reset_index(drop=True)
        combine.to_parquet(f'{FINAL_ALL_PREDICTION_PATH}cd={cd}/p{i}.parquet')
    df = spark.read.parquet(f'{FINAL_ALL_PREDICTION_PATH}cd={cd}/')
    df.write.mode('overwrite').partitionBy('tournamentId').parquet(f'{FINAL_ALL_PREDICTION_TOURNAMENT_PARTITION_PATH}cd={cd}/')


if __name__ == '__main__':
    DATE = sys.argv[1]
    # DATE = "2023-08-15"
    if check_s3_path_exist(f'{TOTAL_INVENTORY_PREDICTION_PATH}cd={DATE}/'):
        combine_inventory_and_sampling(DATE)
    update_dashboards()


