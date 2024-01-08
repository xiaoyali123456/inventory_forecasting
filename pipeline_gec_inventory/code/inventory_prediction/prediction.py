import sys
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
import pandas as pd

from prophet import Prophet
from prophet.diagnostics import cross_validation
from prophet.diagnostics import performance_metrics

from gec_config import *
from gec_path import *
from gec_util import *


def load_prophet_model(changepoint_prior_scale, holidays_prior_scale, weekly_seasonality, yearly_seasonality, holidays, data):
    m = Prophet(changepoint_prior_scale=changepoint_prior_scale, holidays_prior_scale=holidays_prior_scale,
                weekly_seasonality=weekly_seasonality, yearly_seasonality=yearly_seasonality, holidays=holidays)
    m.add_country_holidays(country_name='IN')
    m.fit(data)
    return m


def make_inventory_prediction(forecast_date):
    holidays = pd.read_csv(SUB_HOLIDAYS_FEATURE_PATH)  # TODO: upload this file in the repo
    spark = hive_spark('statistics')
    changepoint_prior_scale = 0.01
    holidays_prior_scale = 10
    yearly_seasonality = False
    period = 90  # days, prediction period? training period? TODO: change to 100
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
            weekly_seasonality = 'auto'  # Q: why auto? what is auto? A: can be True, no too much difference

        # cross validation
        # train the prophet model with period days, and make prediction for the next period days
        # the moving step is also period days.
        # A: why do we need to train the model here? TODO: Need to check
        m = load_prophet_model(changepoint_prior_scale, holidays_prior_scale, weekly_seasonality, yearly_seasonality, holidays, df)
        df_cv = cross_validation(m, initial=f'{period} days', period=f'{period} days', horizon=f'{period} days', parallel="processes")
        df_p = performance_metrics(df_cv, rolling_window=1)
        cross_mape = df_p['mape'].values[0]

        # recent 90 days validation
        m = load_prophet_model(changepoint_prior_scale, holidays_prior_scale, weekly_seasonality, yearly_seasonality, holidays, df[:-1*period])  # use data before last period days for training
        future = m.make_future_dataframe(periods=period)  # make prediction for last period days
        forecast = m.predict(future)  # cover both train and prediction days, i.e. all days
        d = forecast.set_index('ds').join(df.set_index('ds'), how='left', on='ds')
        forecasted = d[-1*period:]  # get the prediction days
        future_mape = (((forecasted.yhat-forecasted.y)/forecasted.y).abs().mean())
        res.append((ad_placement, cross_mape, future_mape))
        reportDF = spark.createDataFrame(pd.DataFrame([[cross_mape, future_mape]], columns = ['cross_validation_mape', 'near_future_map']))
        reportDF.write.mode("overwrite").parquet(f"s3://hotstar-ads-ml-us-east-1-prod/inventory_forecast/gec/report/cd={forecast_date}/ad_placement={ad_placement}")  # TODO: use a path variable instead

        # future prediction
        # train the model by all historic data
        # make prediction for period days in the future
        m = load_prophet_model(changepoint_prior_scale, holidays_prior_scale, weekly_seasonality, yearly_seasonality, holidays, df)
        future = m.make_future_dataframe(periods=period)
        forecast = m.predict(future)  # cover all historic days and future period days
        d = forecast.set_index('ds').join(df.set_index('ds'), how='left', on='ds')
        predictDf = spark.createDataFrame(d.reset_index().drop("cd", axis=1)[['ds', 'trend', 'yhat_lower', 'yhat_upper', 'trend_lower', 'trend_upper',
                         'holidays', 'holidays_lower', 'holidays_upper', 'weekly', 'weekly_lower',
                         'weekly_upper', 'yhat', 'y']]).replace(float('nan'), None)
        predictDf.select('ds', 'trend', 'yhat_lower', 'yhat_upper', 'trend_lower', 'trend_upper',
                         'holidays', 'holidays_lower', 'holidays_upper', 'weekly', 'weekly_lower',
                         'weekly_upper', 'yhat', 'y').write.mode("overwrite").parquet(f"s3://hotstar-ads-ml-us-east-1-prod/inventory_forecast/gec/predicted/cd={forecast_date}/ad_placement={ad_placement}")  # why need select? TODO: use a path variable instead

    # Synchronize the inconsistencies between Hive table metadata and actual data files.
    spark.sql("msck repair table adtech.gec_inventory_forecast_report_daily")  # Q: where is the script of the table creation? A: offline manual creation
    spark.sql("msck repair table adtech.gec_inventory_forecast_prediction_daily")  # enable the dashboard "GEC inventory forecasting monitor" to show the update


def get_inventory_number(date):
    # get inventory number at cd, content and ad_placement level
    inventory_s3_path = f"{INVENTORY_S3_ROOT_PATH}/cd={date}"

    # load inventory event
    # calculate inventory, requests for each (ad_placement, content_id)
    # save in INVENTORY_NUMBER_PATH
    # Q: how to define inventory? a break? A: inventory is number of breaks.
    # Q: what's the relation between request and break? A: for midroll, 1 request vs. multiple breaks; for preroll, 1 request vs. 1 break
    inventory_data = load_data_frame(spark, inventory_s3_path)\
        .groupBy('ad_placement', 'content_id', 'content_type')\
        .agg(F.countDistinct('break_id').alias('inventory'), F.countDistinct('request_id').alias('request_id'))
    save_data_frame(inventory_data, f"{INVENTORY_NUMBER_PATH}/cd={date}")

    # get inventory number at cd and ad_placement level
    # save in GEC_INVENTORY_BY_CD_PATH
    # Q: why apply filter on ad_placement? why don't apply filter during sampling? TODO: remove the filter
    # Q: remove sport_live preroll. how about sport_live midroll? A: sport_live midroll is not in shifu event.
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

    # update inventory number at ad_placement level
    # save in GEC_INVENTORY_BY_AD_PLACEMENT_PATH
    # Q: load all cd data? any efficiency issue? A: no, the data is very small.
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
    sample_date = get_date_list(sys.argv[1], -2)[0]  # TODO: optimize it.

    # get inventory number at cd and ad_placement level (one day)
    # save in GEC_INVENTORY_BY_CD_PATH
    # update inventory number at ad_placement level (all days)
    # save in GEC_INVENTORY_BY_AD_PLACEMENT_PATH
    get_inventory_number(sample_date)

    make_inventory_prediction(sample_date)
    slack_notification(topic=SLACK_NOTIFICATION_TOPIC, region=REGION,
                       message=f"gec prophet prediction on {sample_date} is done.")




