from pyspark.sql.types import StringType, IntegerType
import sys

from gec_config import *
from gec_path import *
from gec_util import *


@F.udf(returnType=StringType())
def merge_ad_placement(raw_ad_placement: str):
    if raw_ad_placement is None:
        return "NULL"
    raw_ad_placement = raw_ad_placement.upper()

    # Q: why these four? A: they are big.
    if raw_ad_placement not in ["BILLBOARD_HOME", "SKINNY_HOME", "PREROLL", "MIDROLL"]:
        return "OTHERS"
    return raw_ad_placement


@F.udf(returnType=StringType())
def parse_language(language):  # TODO: don't need list here
    res = []
    if language:
        language = language.lower()
        if language.startswith("ben"):
            res.append("bengali")
        elif language.startswith("dug"):
            res.append("dugout")
        elif language.startswith("eng"):
            res.append("english")
        elif language.startswith("guj"):
            res.append("gujarati")
        elif language.startswith("hin"):
            res.append("hindi")
        elif language.startswith("kan"):
            res.append("kannada")
        elif language.startswith("mal"):
            res.append("malayalam")
        elif language.startswith("mar"):
            res.append("marathi")
        elif language.startswith("tam"):
            res.append("tamil")
        elif language.startswith("tel"):
            res.append("telugu")
        elif language.startswith("unknown") or language == "":
            res.append("unknown")
        else:
            res.append(language)
    else:
        res.append("unknown")
    return res[0]


@F.udf(returnType=StringType())
def parse_carrier(carrier):
    # Q: why these six? A: probably they are big
    allow_list = ['bsnl', 'vodafone', 'vi', 'idea', 'airtel', 'jio']  # 'others'
    for x in allow_list:
        if x in carrier.lower():  # TODO: move lower outside
            return x
    return 'other'


@F.udf(returnType=StringType())
def parse_device_model(device_model):
    if device_model is None:
        return None
    return device_model.split("%20")[0]


@F.udf(returnType=IntegerType())
def get_hash_and_mod(adv_id, mod_num=100):
    if adv_id is None:
        return VALID_SAMPLE_TAG + 1
    return hash(adv_id) % mod_num


# can be removed, since we introduced bitmap.
@F.udf(returnType=StringType())
def rank_col(col, split_str=','):
    if col is None:
        return ''
    return ','.join(sorted(list(set(col.split(split_str)))))


def load_content_cms(spark):  # content_id, show_id, genre, title, season_no, channel, premium
    spark.sql(f"REFRESH TABLE {EPISODE_TABLE}")  # what will happen if we don't refresh the table?
    spark.sql(f"REFRESH TABLE {MOVIE_TABLE}")
    spark.sql(f"REFRESH TABLE {CLIP_TABLE}")
    cols = ["contentid", "showcontentid", "primarygenre", "title", "seasonno", "channelname", "premium"]
    episode_cms_data = load_hive_table(spark, EPISODE_TABLE)\
        .filter('deleted = False')\
        .select("contentid", "showcontentid", "primarygenre", "title", "seasonno", "channelname", "premium")
    movie_cms_data = load_hive_table(spark, MOVIE_TABLE)\
        .filter('deleted = False')\
        .select("contentid", "showcontentid", "primarygenre", "title", "premium")\
        .withColumn("seasonno", F.lit(None).cast(StringType()))\
        .withColumn("channelname", F.lit(None).cast(StringType()))
    clip_cms_data = load_hive_table(spark, CLIP_TABLE)\
        .filter('deleted = False')\
        .select("contentid", "showcontentid", "primarygenre", "title", "premium")\
        .withColumn("seasonno", F.lit(None).cast(StringType()))\
        .withColumn("channelname", F.lit(None).cast(StringType()))
    # match_cms_data = load_hive_table(spark, "in_cms.match_update_s3")
    # join with cms data to get show_id, season_id,
    cms_data = (episode_cms_data.select(*cols).union(movie_cms_data.select(*cols)).union(clip_cms_data.select(*cols)).
                withColumnRenamed("contentid", "content_id").
                withColumnRenamed("primarygenre", "genre").
                withColumnRenamed("showcontentid", "show_id").
                withColumnRenamed("seasonno", "season_no").
                withColumnRenamed("channelname", "channel").
                distinct()).cache()  # Q: why do we need select(*cols)? A: keep the order
    save_data_frame(cms_data, CMS_DATA_PATH)
    return load_data_frame(spark, CMS_DATA_PATH).cache()


def backup_inventory_data(spark, sample_date, sample_rate):  # remove invalid adv_id and then backup 25% data
    backup_path = BACKUP_PATH + f"_{sample_rate}/cd={sample_date}"  # TODO: using backup_rate instead of sample_rate
    if not check_s3_path_exist(backup_path):
        sample_bucket = int(1/sample_rate)
        print(sample_bucket)
        inventory_s3_path = f"{INVENTORY_S3_ROOT_PATH}/cd={sample_date}"  # this data is generated by ad-serving.

        # inventory_data, count:373,431,543
        # bad adv_id, count: 18,820,947, around 5%
        # after sample: count: 82,083,535
        inventory_data = load_data_frame(spark, inventory_s3_path)\
            .where('adv_id is not null and adv_id != "00000000-0000-0000-0000-000000000000"')\
            .withColumn("sample_id_bucket", get_hash_and_mod('adv_id', F.lit(sample_bucket)))\
            .where(f'sample_id_bucket = {VALID_SAMPLE_TAG}')  # Q: what does a row mean? a request with multiple break? a break? a slot? A: for display, it's an inventory. for vod, it's an ad break.
        save_data_frame(inventory_data, backup_path)


def sample_data_daily(spark, sample_date, cms_data, sample_rate=100):  # further sample 1% data
    if not check_s3_path_exist(SAMPLING_DATA_NEW_PATH + f"/cd={sample_date}"):
        inventory_s3_path = f"{BACKUP_PATH}_0.25/cd={sample_date}"  # TODO: 0.25 should be replaced with backup rate

        # sample 1% inventory, i.e. 4% backed inventory
        # Q: remove sport_live preroll. how about sport_live midroll? A: shifu contains all preroll ads
        inventory_data = load_data_frame(spark, inventory_s3_path) \
            .withColumn("sample_id_bucket", get_hash_and_mod('adv_id', F.lit(sample_rate)))\
            .where(f'sample_id_bucket = {VALID_SAMPLE_TAG}')\
            .withColumn("ad_placement", merge_ad_placement('ad_placement'))\
            .where('ad_placement != "PREROLL" or (ad_placement = "PREROLL" and lower(content_type) != "sport_live")')\
            .withColumn("date", F.lit(sample_date))\
            .withColumn("device_carrier", parse_carrier('device_carrier'))\
            .withColumn("device_model", parse_device_model('device_model'))\
            .withColumn("content_language", parse_language('content_language'))\
            .cache()

        # add content attributes to inventory data
        # Q: can have multiple age_buckets? A: from difference sources, or broad and standard
        # Q: why random sample_id_bucket? A: No need, can be removed
        enriched_inventory_data = inventory_data.join(F.broadcast(cms_data), "content_id", how="left_outer") \
            .selectExpr('adv_id', 'lower(city) as city', 'lower(state) as state',
                        'lower(location_cluster) as location_cluster',
                        'lower(pincode) as pincode',
                        'lower(demo_gender) as demo_gender', 'lower(split(demo_gender, ",")[0]) as gender',
                        'lower(demo_age_range) as demo_age_range', 'lower(split(demo_age_range, ",")[0]) as age_bucket',
                        'demo_source', 'lower(ibt) as ibt', 'lower(device_brand) as device_brand',
                        'lower(device_model) as device_model',
                        'device_carrier', 'lower(device_network_data) as device_network_data',
                        'lower(user_account_type) as user_account_type', 'lower(device_platform) as device_platform',
                        'lower(device_os_version) as device_os_version', 'lower(device_app_version) as device_app_version',
                        'ad_placement', 'content_id', 'lower(content_type) as content_type', 'content_language',
                        'request_id', 'break_id', 'break_slot_count', 'date', 'hr', 'sample_id_bucket', 'show_id', 'lower(genre) as genre', 'lower(title) as title', 'season_no',
                        'custom_tags', 'user_segment') \
            .fillna('', SAMPLING_COLS + CMS_COLS + ['demo_age_range', 'custom_tags', 'user_segment']) \
            .withColumn('ibt', rank_col('ibt')) \
            .withColumn('age_bucket', rank_col('demo_age_range')) \
            .withColumn('sample_id_bucket', F.rand()) \
            .withColumn('day_of_week', F.dayofweek(F.col('date')))
        print(enriched_inventory_data.count())
        print(enriched_inventory_data.where('age_bucket = "" or gender = ""').count())

        # save inventory (sample count) and reach of each ad_placement in SAMPLING_DATA_SUMMARY_PATH
        # save enriched inventory data in SAMPLING_DATA_NEW_PATH
        save_data_frame(enriched_inventory_data.groupBy('ad_placement').agg(F.count("*").alias("inventory_sample_count"), F.countDistinct("adv_id").alias("reach_sample_count")), SAMPLING_DATA_SUMMARY_PATH + f"/cd={sample_date}")
        save_data_frame(enriched_inventory_data, SAMPLING_DATA_NEW_PATH + f"/cd={sample_date}")


@F.udf(returnType=StringType())
def extract_targeting_with_special_prefix(col, prefix, split_str=','):
    if col is None:
        return ''
    targeting_list = list(set(col.split(split_str)))
    res = []
    for targeting in targeting_list:
        if targeting.startswith(prefix):
            res.append(targeting)
    return ','.join(res)


@F.udf(returnType=StringType())
def extract_device_price(col, split_str=','):
    if col is None:
        return ''
    targeting_list = list(set(col.split(split_str)))
    device_mapping = {'A_53374664': '<10K',
                      'A_40894273': '10K-15K',
                      'A_15031263': '15K-20K',
                      'A_94523754': '20K-25K',
                      'A_40990869': '25K-35K',
                      'A_21231588': '35K+'}
    res = []
    for targeting in targeting_list:
        if targeting in device_mapping:
            res.append(device_mapping[targeting])
    return ','.join(res)


def check_if_3rd_party_targeting(input_str):
    if len(input_str) != 3 or (input_str[0] != 'P' and input_str[0] != 'E'):
        return False
    if input_str[1].isdigit() and input_str[2].isdigit():
        return True
    if input_str[1].isupper() and input_str[2].isupper():
        return True
    if input_str[1].isdigit() and input_str[2].isupper():
        return True
    if input_str[1].isupper() and input_str[2].isdigit():
        return True
    return False


@F.udf(returnType=StringType())
def extract_3rd_party_cohorts(col, split_str=','):
    if col is None:
        return ''
    targeting_list = list(set(col.split(split_str)))
    res = []
    for targeting in targeting_list:
        if check_if_3rd_party_targeting(targeting):
            res.append(targeting)
    return ','.join(res)


# sample rate = 1/200, about 570k rows, about 30MB
# TODO need to daily join shifu ad_insertion table to get the max_slot_number and max_break_duration for preroll and midroll
def generate_vod_sampling_and_aggr_on_content(spark, sample_date, cms_data, sample_rate=300):
    res = load_data_frame(spark, SAMPLING_DATA_NEW_PATH + f"/cd={sample_date}") \
        .where("ad_placement in ('PREROLL', 'MIDROLL')")
    aggr_cols = ['content_id', 'adv_id', 'city', 'state', 'location_cluster', 'gender', 'age_bucket',
            'ibt', 'device_brand', 'device_model', 'device_carrier', 'user_account_type',
            'device_platform', 'ad_placement', 'content_type', 'content_language', 'break_slot_count',
            'show_id', 'genre', 'season_no', 'channel', 'premium', 'nccs', 'device_price', '3rd_party_cohorts']

    # sampled 1/300 preroll and midroll inventory
    # Q: why join cms_data again? A: No need. can lower the channel when enrich the data.
    # Q: why hash adv_id with MAX_INT-2? A: convert a string to an int to save storage.
    # calculate break_num and break_slot_count for each (content_id, adv_id)
    # Q: how to ensure vod? A: only focus on preroll and midroll
    df = res\
        .withColumn("sample_id_bucket", get_hash_and_mod('adv_id', F.lit(sample_rate))) \
        .where(f'sample_id_bucket = 1')\
        .join(F.broadcast(cms_data.selectExpr('content_id', 'lower(channel) as channel', 'premium')), "content_id", how="left_outer")\
        .withColumn('nccs', extract_targeting_with_special_prefix('user_segment', F.lit("NCCS_")))\
        .withColumn('device_price', extract_device_price('user_segment'))\
        .withColumn('3rd_party_cohorts', extract_3rd_party_cohorts('user_segment')) \
        .withColumn('adv_id', get_hash_and_mod('adv_id', F.lit(MAX_INT-2))) \
        .groupBy(aggr_cols) \
        .agg(F.count('*').alias('break_num'))\
        .withColumn('break_slot_count', F.expr('break_slot_count * break_num'))
    # 'dt', 'city', 'state', 'location_cluster', 'gender', 'age_bucket', 'ibt', 'device_brand',
    # 'device_model', 'device_carrier', 'device_network_data', 'user_account_type', 'device_platform',
    # 'device_os_version', 'device_app_version', 'ad_placement', 'content_id', 'content_type', 'content_language',
    # 'break_slot_count', 'date', 'show_id', 'genre', 'adv_id', 'season_no', 'day_of_week', 'break_num'
    # df_for_csv.show(20, False)
    # save_data_frame(df_for_csv, VOD_SAMPLING_DATA_PREDICTION_CSV_PATH + f"/cd={sample_date}", "csv", True)

    # save in VOD_SAMPLING_DATA_PREDICTION_PARQUET_PATH
    save_data_frame(df, VOD_SAMPLING_DATA_PREDICTION_PARQUET_PATH + f"_sample_rate_{sample_rate}/cd={sample_date}")


if __name__ == '__main__':
    sample_date = get_date_list(sys.argv[1], -2)[0]  # TODO: optimize it
    spark = hive_spark("gec_sampling")

    # load inventory event from INVENTORY_S3_ROOT_PATH, remove invalid adv_id and then sample 25% data
    # save in BACKUP_PATH
    backup_inventory_data(spark, sample_date, 0.25)

    # load (content_id, show_id, genre, title, season_no, channel, premium) of episode, movie and clip
    # save in CMS_DATA_PATH
    cms_data = load_content_cms(spark)

    # load backup inventory data, and sample 1% of total inventory data
    # enrich inventory data with content attributes
    # save ad placement statistics in SAMPLING_DATA_SUMMARY_PATH
    # save enriched inventory data in SAMPLING_DATA_NEW_PATH
    sample_data_daily(spark, sample_date, cms_data)

    # load enriched inventory data, and sample 1/300 of total inventory data
    # get vod preroll and midroll inventory and calculate break_num and break_slot num for (adv_id, content_id)
    # save the result in VOD_SAMPLING_DATA_PREDICTION_PARQUET_PATH
    generate_vod_sampling_and_aggr_on_content(spark, sample_date, cms_data)
    slack_notification(topic=SLACK_NOTIFICATION_TOPIC, region=REGION,
                       message=f"gec_sampling on {sample_date} is done.")
