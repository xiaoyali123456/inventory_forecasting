from .watched_video import prepare_palyout_df

concurrency_root_path = "s3://hotstar-dp-datalake-processed-us-east-1-prod/hive_internal_database/concurrency.db/"
ssai_concurrency_path = f"{concurrency_root_path}users_by_live_sports_content_by_ssai/"

dt = '2022-11-13'
concurrency_df = spark.read.parquet(f"{ssai_concurrency_path}/cd={dt}/")

'''
+--------------------+----------+----------------+-------+--------------------+---+
|                time|content_id|content_language|no_user|            ssai_tag| hr|
+--------------------+----------+----------------+-------+--------------------+---+
|2022-11-13T11:00:00Z|1540019041|         english|    1.0|SSAI::D_NA:G_U:S_...| 11|
|2022-11-13T11:00:00Z|1540019053|           hindi|    1.0|SSAI::D_NA:G_U:S_...| 11|
'''

concurrency_df.join()
