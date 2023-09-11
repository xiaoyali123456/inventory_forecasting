"""
    1.calculate inventory&reach for each cohort of each match
    2.need to scale the inventory&reach when the languages or platform of the match is uncompleted
"""
import sys

import pandas as pd
import pyspark.sql.functions as F

from util import *
from path import *


def parse(string):
    if string is None or string == '':
        return False
    lst = [x.lower() for x in json.loads(string)]
    return lst


def combine_inventory_and_sampling(cd):
    model_predictions = spark.read.parquet(f'{TOTAL_INVENTORY_PREDICTION_PATH}cd={cd}/')
    reach_ratio = spark.read.parquet(f'{PREROLL_REACH_RATIO_RESULT_PATH}cd={cd}/')
    ad_time_ratio = spark.read.parquet(f'{PREROLL_INVENTORY_RATIO_RESULT_PATH}cd={cd}/')
    ad_time_ratio = ad_time_ratio.withColumnRenamed('ad_time', 'inventory')
    processed_input = spark.read.parquet(PREPROCESSED_INPUT_PATH + f'cd={cd}/')
    # sampling match one by one
    for i, row in model_predictions.toPandas().iterrows():
        reach = reach_ratio.select("*")
        inventory = ad_time_ratio.select("*")
        # calculate predicted inventory and reach for each cohort
        reach = reach.withColumn('reach', reach['reach'] * row['estimated_reach'])
        inventory = inventory.withColumn('inventory', inventory['inventory'] * row['estimated_preroll_inventory'])
        common_cols = list(set(reach.columns) & set(inventory.columns))
        combine = inventory.join(reach, on=common_cols, how='left')
        # add meta data for each match
        combine = combine.withColumn('request_id', F.lit(str(row['request_id'])))
        combine = combine.withColumn('matchId', F.lit(int(row['match_id'])))
        meta_info = processed_input.filter(processed_input['matchId'] == row['match_id']).first()
        combine = combine.withColumn('tournamentId', F.lit(meta_info['tournamentId']))
        combine = combine.withColumn('seasonId', F.lit(meta_info['seasonId']))
        combine = combine.withColumn('adPlacement', F.lit('PREROLL'))
        # process cases when languages of this match are incomplete
        languages = parse(meta_info['contentLanguages'])
        # print(languages)
        # combine.show()
        if languages:
            language_sum = combine.groupby('adPlacement').agg(F.sum('reach'), F.sum('inventory')).collect()[0]
            filter_language_sum = combine.filter(F.col('language').isin(languages)).groupby('adPlacement').agg(F.sum('reach'), F.sum('inventory')).collect()[0]
            # print(language_sum)
            combine = combine.withColumn('reach', combine['reach'] * language_sum[1] / filter_language_sum[1])
            combine = combine.withColumn('inventory', combine['inventory'] * language_sum[2] / filter_language_sum[2])
            combine = combine.filter(F.col('language').isin(languages))
        # process case when platforms of this match are incomplete
        platforms = parse(meta_info['platformsSupported'])
        # print(platforms)
        if platforms:
            platform_sum = combine.groupby('adPlacement').agg(F.sum('reach'), F.sum('inventory')).collect()[0]
            filter_platform_sum = combine.filter(F.col('platform').isin(platforms)).groupby('adPlacement').agg(F.sum('reach'),
                                                                                       F.sum('inventory')).collect()[0]
            # print(platform_sum)
            combine = combine.withColumn('reach', combine['reach'] * platform_sum[1] / filter_platform_sum[1])
            combine = combine.withColumn('inventory', combine['inventory'] * platform_sum[2] / filter_platform_sum[2])
            combine = combine.filter(F.col('platform').isin(platforms))
        combine = combine.withColumn('inventory', combine['inventory'].cast('integer'))
        combine = combine.withColumn('reach', combine['reach'].cast('integer'))
        # combine = combine.replace(
        #    {'device': {'15-20K': 'A_15031263', '20-25K': 'A_94523754', '25-35K': 'A_40990869', '35K+': 'A_21231588'}},
        #    subset=['device']
        # )
        combine = combine.withColumnRenamed('age', 'ageBucket')
        combine = combine.withColumnRenamed('device', 'devicePrice')
        combine = combine.withColumnRenamed('request_id', 'inventoryId')
        combine = combine.withColumnRenamed('custom_cohorts', 'customCohort')
        combine = combine.filter((combine['inventory'] >= 1) & (combine['reach'] >= 1) & (F.length(combine['city']) != 1))
        save_data_frame(combine, f'{FINAL_ALL_PREROLL_PREDICTION_PATH}cd={cd}/saved_id={i}')
    df = spark.read.option("mergeSchema", "true").parquet(f'{FINAL_ALL_PREROLL_PREDICTION_PATH}cd={cd}/')
    df.write.mode('overwrite').partitionBy('tournamentId').parquet(f'{FINAL_ALL_PREROLL_PREDICTION_TOURNAMENT_PARTITION_PATH}cd={cd}/')


if __name__ == '__main__':
    DATE = sys.argv[1]
    # DATE = "2023-08-15"
    if check_s3_path_exist(f'{TOTAL_INVENTORY_PREDICTION_PATH}cd={DATE}/'):
        combine_inventory_and_sampling(DATE)
    update_dashboards()


