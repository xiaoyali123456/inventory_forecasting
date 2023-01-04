import json
import pandas as pd
import pyspark.sql.functions as F

concurrency_root_path = "s3://hotstar-dp-datalake-processed-us-east-1-prod/hive_internal_database/concurrency.db/"
ssai_concurrency_path = f"{concurrency_root_path}users_by_live_sports_content_by_ssai/"
playout_log_path = 's3://hotstar-ads-data-external-us-east-1-prod/run_log/blaze/prod/test/'
output_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/sampling/'
match_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/sampling/match_df/'

def load_datetime(time_str):
    try:
        return pd.to_datetime(time_str)
    except:
        return None

def load_playout_time(date_col, time_col):
    ts = (date_col + ' ' + time_col).apply(load_datetime)
    return pd.Series(ts.dt.tz_localize('Asia/Kolkata').dt.tz_convert(None).dt.to_pydatetime(), dtype=object)

def prepare_playout_df(dt):
    playout_df = spark.read.csv(f'{playout_log_path}{dt}', header=True).toPandas()
    playout_df['break_start'] = load_playout_time(playout_df['Start Date'], playout_df['Start Time'])
    playout_df['break_end'] = load_playout_time(playout_df['End Date'], playout_df['End Time'])
    playout_df = playout_df[~(playout_df.break_start.isna()|playout_df.break_end.isna())]
    playout_df.rename(columns={
        'Content ID': 'content_id',
        'Playout ID': 'playout_id',
        'Language': 'language',
        'Tenant': 'country',
        'Platform': 'platform',
    }, inplace=True)
    playout_df.language = playout_df.language.str.lower()
    playout_df.platform = playout_df.platform.str.split('|')
    return playout_df.explode('platform')

def main():
    tournament='wc2022'
    with open('dates.json') as f:
        dates = json.load(f)
    dates.remove('2022-10-17')
    for dt in dates:
        playout = prepare_playout_df(dt)
        playout = playout[playout.platform == 'android'] # TODO: hack, select 1 palyout only
        playout = spark.createDataFrame(playout[['content_id', 'language', 'break_start', 'break_end']])
        concurrency_df = spark.read.parquet(f"{ssai_concurrency_path}/cd={dt}/")
        concurrency_df = concurrency_df.withColumn('time', F.from_utc_timestamp(F.col('time'), F.lit('UTC')))
        final_path = f'{output_path}/inventory_concurrency/cd={dt}/'
        match_df = spark.read.parquet(match_path) \
            .where('title not like "%follow on%" and title not like "%warm-up%"')
        concurrency_df.withColumnRenamed('content_language', 'language') \
            .withColumn('no_user', F.col('no_user').cast('float')) \
            .join(match_df[['content_id']], 'content_id') \
            .join(playout, on=['content_id', 'language']) \
            .where('break_start <= time and time <= break_end') \
            .groupby('content_id', 'ssai_tag').sum('no_user') \
            .withColumnRenamed('sum(no_user)', 'ad_time') \
            .withColumn('ad_time', F.col('ad_time')*60) \
            .write.parquet(final_path)

if __name__ == '__main__':
    main()
