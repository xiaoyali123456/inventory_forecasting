import sys

import pyspark.sql.functions as F
from pyspark.sql.types import StringType

from util import *
from path import *
from config import *


def should_be_used_season(sport_season_name):
    if isinstance(sport_season_name, str):
        sport_season_name = sport_season_name.lower()
        tournaments = [
            'icc world test championship',
            "icc men's cwc qualifier",
        ] + FOCAL_TOURNAMENTS_FOR_SAMPLING
        for t in tournaments:
            if t in sport_season_name:
                return True
    return False


def load_new_matches(cd):
    last_cd = get_last_cd(PREROLL_SAMPLING_PATH, cd)
    if last_cd is None:
        last_cd = '2022-10-15' # 10-16 is world cup 22
    matches = spark.read.parquet(MATCH_CMS_PATH_TEMPL % cd) \
        .where(f'startdate > "{last_cd}" and startdate < "{cd}"').toPandas()
    last_available_date = (datetime.today()-timedelta(90)).strftime('%Y-%m-%d') # S3 glacier deadline
    matches = matches[matches.startdate >= last_available_date]
    return matches[matches.sportsseasonname.map(should_be_used_season)]


@F.udf(returnType=StringType())
def parse_carrier(carrier):
    allow_list = ['bsnl', 'vodafone', 'vi', 'idea', 'airtel', 'jio'] # 'others'
    for x in allow_list:
        if x in carrier.lower():
            return x
    return 'other'


@F.udf(returnType=StringType())
def languages_process(language):
    res = []
    if language:
        language = language.lower()
        if language.startswith("ben"):
            res.append("bengali")
        elif language.startswith("dug"):
            res.append("dugout")
        elif language.startswith("eng") or language.startswith("الإنجليزية"):
            res.append("english")
        elif language.startswith("guj"):
            res.append("gujarati")
        elif language.startswith("hin") or language.startswith("הינדי") or language.startswith("हिन्दी"):
            res.append("hindi")
        elif language.startswith("kan"):
            res.append("kannada")
        elif language.startswith("mal") or language.startswith("മലയാളം"):
            res.append("malayalam")
        elif language.startswith("mar"):
            res.append("marathi")
        elif language.startswith("tam") or language.startswith("தமிழ்"):
            res.append("tamil")
        elif language.startswith("tel") or language.startswith("తెలుగు"):
            res.append("telugu")
        elif language.startswith("unknown") or language == "":
            res.append("unknown")
        else:
            res.append(language)
    else:
        res.append("unknown")
    return res[0]


def process(cd, content_ids):
    print('process', cd)
    print('begin')
    final_output_path = f'{PREROLL_SAMPLING_PATH}cd={cd}/'
    success_path = f'{final_output_path}_SUCCESS'
    if s3.isfile(success_path):
        print('skip')
        return
    preroll = spark.read.parquet(PREROLL_INVENTORY_PATH + f'cd={cd}/')\
        .where(F.col('content_id').isin(content_ids) & F.expr("lower(ad_placement) = 'preroll'"))\
        .where("lower(ad_placement) = 'preroll' and lower(content_type) = 'sport_live'")\
        .groupby(
        F.expr('lower(split(demo_gender, ",")[0]) as gender'),
        F.expr('lower(split(demo_age_range, ",")[0]) as age_bucket'),
        F.expr('lower(city) as city'),
        F.expr('lower(state) as state'),
        F.expr('lower(location_cluster) as location_cluster'),
        F.expr('lower(pincode) as pincode'),
        F.expr("lower(ibt) as interest"), # separated by comma
        F.expr('lower(device_brand) as device_brand'),
        F.expr('lower(device_model) as device_model'),
        languages_process('content_language').alias('language'),
        parse_carrier('device_carrier').alias('primary_sim'),
        F.expr('lower(device_network_data) as data_sim'),
        F.expr('lower(device_platform) as platform'),
        F.expr('lower(device_os_version) as os'),
        F.expr('lower(device_app_version) as app_version'),
        F.expr('lower(user_account_type) as subscription_type'),
        F.expr('lower(content_type) as content_type'),
        F.lit('cricket').alias('content_genre'),
    ).agg(F.count('dw_d_id').alias('ad_time'), F.expr('count(distinct dw_d_id) as reach'))
    preroll.repartition(64).write.mode('overwrite').parquet(final_output_path)
    print('end')


def main(cd):
    matches = load_new_matches(cd)
    print(matches)
    if len(matches):
        for dt in matches.startdate.drop_duplicates():
            content_ids = matches[matches.startdate == dt].content_id.tolist()
            process(dt, content_ids)


if __name__ == '__main__':
    cd = sys.argv[1]
    main(cd)
