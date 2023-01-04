import json
import os
from datetime import datetime

output_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/watched_video/'
playout_log_path = 's3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test/'
wt_path = 's3://hotstar-dp-datalake-processed-us-east-1-prod/events/watched_video/'

# import pyspark.sql.functions as F
# from pyspark.sql.types import BooleanType

# @F.udf(returnType=BooleanType())
# def is_valid_title(title):
#     for arg in ['warm-up', 'follow on']:
#         if arg in title:
#             return False
#     return ' vs ' in title

# def valid_dates(tournament, save=False):
#     match_meta_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/data/ads_crash/match_meta'
#     tournament_dic = {'wc2022': 'ICC Men\'s T20 World Cup 2022',
#                     'ipl2022': 'TATA IPL 2022',
#                     'wc2021': 'ICC Men\'s T20 World Cup 2021',
#                     'ipl2021': 'VIVO IPL 2021'}
#     match_df = spark.read.parquet(match_meta_path) \
#         .where(f'shortsummary="{tournament_dic[tournament]}" and contenttype="SPORT_LIVE"') \
#         .selectExpr('substring(from_unixtime(startdate), 1, 10) as date',
#                     'contentid as content_id',
#                     'lower(title) as title',
#                     'shortsummary') \
#         .where(is_valid_title('title')) \
#         .orderBy('date') \
#         .distinct()
#     if save:
#         match_df.write.mode('overwrite').parquet(f'{output_path}match_df/tournament={tournament}/')
#     return match_df.select('date').distinct().toPandas()['date'] #TODO: this is UTC but playout is IST

def main():
    tournament='wc2022'
    if os.path.exists('dates.json'):
        with open('dates.json') as fp:
            dates = json.load(fp)
    else:
        dates = list(valid_dates(tournament))
        with open('dates.json', 'w') as fp:
            json.dump(dates, fp)
    for dt in dates:
        print(dt)
        for i in range(24):
            print(datetime.now(), 'hr:', i)
            path=f'{wt_path}cd={dt}/hr={i:02}'
            path2=f'{output_path}cd={dt}/hr={i:02}'
            if os.system(f'aws s3 ls {path2}/_SUCCESS'):
                os.system(f'aws s3 sync {path} {path2}')
        
if __name__ == '__main__':
    main()
