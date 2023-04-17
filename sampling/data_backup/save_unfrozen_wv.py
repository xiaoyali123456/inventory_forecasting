import os
import pandas as pd
import pyspark.sql.functions as F

src = 's3://hotstar-dp-datalake-processed-us-east-1-prod/events/watched_video/'

dates = [
    ['2019-05-29', '2019-07-15'],
    ['2019-12-05', '2019-12-23'],
    ['2020-01-04', '2020-02-12'],
    ['2020-09-18', '2020-11-11'],
    ['2021-02-04', '2021-03-29'],
    ['2021-04-08', '2021-05-04'],
]

def save_quarter():
    dst = 's3://hotstar-ads-ml-us-east-1-prod/data_exploration/data/data_backup/watched_video/'
    user_last_char = ['2', 'a', 'e', '8']
    for pair in dates:
        run = pd.date_range(pair[0], pair[1])
        print(run[0], run[-1])
        for x in run:
            print('running', x.date())
            final = f'{dst}cd={x.date()}/'
            if os.system(f'aws s3 ls {final}_SUCCESS') == 0:
                print(x.date(), 'exists')
                continue
            spark.read.parquet(f'{src}cd={x.date()}') \
                .withColumn('dw_p_id_prefix', F.col('dw_p_id').substr(-1, 1)) \
                .filter(F.col('dw_p_id_prefix').isin(user_last_char)) \
                .write.mode('overwrite').partitionBy('hr', 'dw_p_id_prefix').parquet(final)

# save_quarter()

def save_cols():
    for x in pd.date_range(dates[0][0], dates[0][1]):
        for h in range(24):
            inp = src + f'cd={x.date()}/hr={h:02}/'
            dst = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/watched_video_subset/'
            out = dst + f'cd={x.date()}/hr={h:02}/'
            print(out)
            if os.system(f'aws s3 ls {out}_SUCCESS') == 0:
                print('exists')
                continue
            spark.read.parquet(inp)[['dw_d_id', 'language', 'platform', 'country', 'content_id', 'timestamp', 'received_at', 'watch_time', 'state', 'city', 'gender']] \
                .write.mode('overwrite').parquet(out)

save_cols()

# backup one day of CWC 2019
def save_user_seg():
    dst = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/watched_video_subset/compare/'
    date = 'cd=2019-06-16/'
    for i in range(24):
        spark.read.parquet(src+date+f'hr={i:02}/')[[
            'dw_d_id', 'language', 'platform', 'country',
            'content_id', 'timestamp', 'received_at', 'watch_time',
            'state', 'city', 'gender', 'user_segments'
        ]].write.parquet(dst+date+f'hr={i:02}/')
