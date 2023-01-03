from .watched_video import prepare_palyout_df

concurrency_root_path = "s3://hotstar-dp-datalake-processed-us-east-1-prod/hive_internal_database/concurrency.db/"
ssai_concurrency_path = f"{concurrency_root_path}users_by_live_sports_content_by_ssai/"

dt = '2022-11-13'
concurrency_df = spark.read.parquet(f"{ssai_concurrency_path}/cd={dt}/")
concurrency_df.where('')
