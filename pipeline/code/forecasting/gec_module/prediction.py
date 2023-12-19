import sys
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
import pandas as pd

from prophet import Prophet
from prophet.diagnostics import cross_validation
from prophet.diagnostics import performance_metrics

from config import *
from path import *
from util import *


def load_prophet_model(changepoint_prior_scale, holidays_prior_scale, weekly_seasonality, yearly_seasonality, holidays, data):
    m = Prophet(changepoint_prior_scale=changepoint_prior_scale, holidays_prior_scale=holidays_prior_scale,
                weekly_seasonality=weekly_seasonality, yearly_seasonality=yearly_seasonality, holidays=holidays)
    m.add_country_holidays(country_name='IN')
    m.fit(data)
    return m


def inventory_prediction(forecast_date):
    holidays = pd.read_csv(SUB_HOLIDAYS_FEATURE_PATH)
    spark = hive_spark('statistics')
    changepoint_prior_scale = 0.01
    holidays_prior_scale = 10
    yearly_seasonality = False
    period = 90
    res = []
    for ad_placement in ["OTHERS", "MIDROLL", "BILLBOARD_HOME", "SKINNY_HOME", "PREROLL"]:
        print(ad_placement)
        f = GEC_INVENTORY_BY_AD_PLACEMENT_PATH + "/ad_placement=" + ad_placement
        df = pd.read_parquet(f)
        df = df.sort_values(by=['cd'])
        df['ds'] = pd.to_datetime(df['cd'])
        if ad_placement in ["MIDROLL", "PREROLL"]:
            weekly_seasonality = True
        else:
            weekly_seasonality = 'auto'
        # cross validation
        m = load_prophet_model(changepoint_prior_scale, holidays_prior_scale, weekly_seasonality, yearly_seasonality, holidays, df)
        # df_cv = cross_validation(m, initial='365 days', period='365 days', horizon='365 days', parallel="processes")
        df_cv = cross_validation(m, initial=f'{period} days', period=f'{period} days', horizon=f'{period} days', parallel="processes")
        df_p = performance_metrics(df_cv, rolling_window=1)
        cross_mape = df_p['mape'].values[0]
        # recent 90 days validation
        m = load_prophet_model(changepoint_prior_scale, holidays_prior_scale, weekly_seasonality, yearly_seasonality, holidays, df[:-1*period])
        future = m.make_future_dataframe(periods=period)
        forecast = m.predict(future)
        d = forecast.set_index('ds').join(df.set_index('ds'), how='left', on='ds')
        forecasted = d[-1*period:]
        future_mape = (((forecasted.yhat-forecasted.y)/forecasted.y).abs().mean())
        res.append((ad_placement, cross_mape, future_mape))
        reportDF = spark.createDataFrame(pd.DataFrame([[cross_mape, future_mape]], columns = ['cross_validation_mape', 'near_future_map']))
        reportDF.write.mode("overwrite").parquet(f"s3://hotstar-ads-ml-us-east-1-prod/inventory_forecast/gec/report/cd={forecast_date}/ad_placement={ad_placement}")
        # future prediction
        m = load_prophet_model(changepoint_prior_scale, holidays_prior_scale, weekly_seasonality, yearly_seasonality, holidays, df)
        future = m.make_future_dataframe(periods=period)
        forecast = m.predict(future)
        d = forecast.set_index('ds').join(df.set_index('ds'), how='left', on='ds')
        predictDf = spark.createDataFrame(d.reset_index().drop("cd", axis=1)[['ds', 'trend', 'yhat_lower', 'yhat_upper', 'trend_lower', 'trend_upper',
                         'holidays', 'holidays_lower', 'holidays_upper', 'weekly', 'weekly_lower',
                         'weekly_upper', 'yhat', 'y']]).replace(float('nan'), None)
        predictDf.select('ds', 'trend', 'yhat_lower', 'yhat_upper', 'trend_lower', 'trend_upper',
                         'holidays', 'holidays_lower', 'holidays_upper', 'weekly', 'weekly_lower',
                         'weekly_upper', 'yhat', 'y').write.mode("overwrite").parquet(f"s3://hotstar-ads-ml-us-east-1-prod/inventory_forecast/gec/predicted/cd={forecast_date}/ad_placement={ad_placement}")
    spark.sql("msck repair table adtech.gec_inventory_forecast_report_daily")
    spark.sql("msck repair table adtech.gec_inventory_forecast_prediction_daily")


def get_inventory_number(date):
    inventory_s3_path = f"{INVENTORY_S3_ROOT_PATH}/cd={date}"
    inventory_data = load_data_frame(spark, inventory_s3_path)\
        .groupBy('ad_placement', 'content_id', 'content_type')\
        .agg(F.countDistinct('break_id').alias('inventory'), F.countDistinct('request_id').alias('request_id'))
    save_data_frame(inventory_data, f"{INVENTORY_NUMBER_PATH}/cd={date}")
    df = load_data_frame(spark, f"{INVENTORY_NUMBER_PATH}/cd={date}")\
        .filter(F.upper(col("ad_placement")).isin(supported_ad_placement)) \
        .withColumn("ad_placement", merge_ad_placement_udf('ad_placement')) \
        .fillna('', ['content_type']) \
        .where('ad_placement != "PREROLL" or (ad_placement = "PREROLL" and lower(content_type) != "sport_live")') \
        .groupBy('ad_placement') \
        .agg(F.sum('inventory').alias('y')) \
        .cache()
    df.show(20, False)
    save_data_frame(df, f"{GEC_INVENTORY_BY_CD_PATH}/cd={date}")
    # load_data_frame(spark, f"{GEC_INVENTORY_BY_CD_PATH}").where('cd > "2023-12-01"').show(20, False)
    save_data_frame(load_data_frame(spark, f"{GEC_INVENTORY_BY_CD_PATH}"),
                    GEC_INVENTORY_BY_AD_PLACEMENT_PATH, partition_col='ad_placement')


def merge_ad_placement(raw_ad_placement: str):
    if raw_ad_placement is None:
        return "Null"
    raw_ad_placement = raw_ad_placement.upper()
    if raw_ad_placement not in ["BILLBOARD_HOME", "SKINNY_HOME", "PREROLL", "MIDROLL"]:
        return "OTHERS"
    return raw_ad_placement


merge_ad_placement_udf = F.udf(merge_ad_placement, StringType())


if __name__ == '__main__':
    sample_date = get_date_list(sys.argv[1], -2)[0]
    get_inventory_number(sample_date)
    inventory_prediction(sample_date)
    slack_notification(topic=SLACK_NOTIFICATION_TOPIC, region=REGION,
                       message=f"gec prophet prediction on {sample_date} is done.")




