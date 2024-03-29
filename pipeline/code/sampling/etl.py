"""
 1. generate table ('content_id', 'playout_id', 'language', 'platform', 'country', 'city', 'state', 'cohort', 'ad_time', 'reach')
 for regular cohorts distribution in terms of finished matches
    1.1. filter the matches that are not calculated and belongs to important tournaments
    1.2. load playout table and watch_video table
    1.3. parse user segments col in watch_video table to get the cohort info
    1.4. join these 2 tables to calculate ad_time and reach
    cohort="A_15031263|NCCS_A|..."

 2. generate table ('is_cricket', 'segments', 'watch_time', 'reach') for custom cohorts distribution in terms of recent five days
    2.1. load segment->ssai mapping from request
    2.2. load user-segment table and convert segments to ssai tag
    2.3. load watch_video table for recent 5 days
    2.4. join these 2 tables to calculate watch_time and reach
    segments="C14_1|C15_2"
"""
import sys

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import *

from util import *
from path import *
from config import *


def extract_segments(lst):
    filtered = set()
    equals = ['A_15031263', 'A_94523754', 'A_40990869', 'A_21231588']  # device price
    prefixs = ['NCCS_', 'CITY_', 'STATE_', 'FMD00', 'MMD00', 'P_', 'R_F', 'R_M']
    # prefixs = ['NCCS_', 'CITY_', 'STATE_', 'FMD00', 'MMD00', 'P_']
    middles = ['_MALE_', '_FEMALE_']
    for t in lst:
        match = False
        for s in equals:
            if t == s:
                match = True
                break
        if not match:
            for s in prefixs:
                if t.startswith(s):
                    match = True
                    break
        if not match:
            for s in middles:
                if s in t:
                    match = True
                    break
        if match:
            filtered.add(t)
    return '|'.join(sorted(filtered))


def convert_short_tag_to_log_tag(lst):
    demo_short_to_long = {
        'DF01': 'FB_MALE_13-17',
        'DF02': 'FB_MALE_18-24',
        'DF03': 'FB_MALE_25-34',
        'DF04': 'FB_MALE_35-44',
        'DF05': 'FB_MALE_45-54',
        'DF06': 'FB_MALE_55-64',
        'DF07': 'FB_MALE_65PLUS',
        'DF11': 'FB_MALE_TV_2-14',
        'DF12': 'FB_MALE_TV_15-21',
        'DF13': 'FB_MALE_TV_22-30',
        'DF14': 'FB_MALE_TV_31-40',
        'DF15': 'FB_MALE_TV_41-50',
        'DF16': 'FB_MALE_TV_51-60',
        'DF17': 'FB_MALE_TV_61PLUS',
        'DF21': 'FB_BARC_MALE_15-21',
        'DF22': 'FB_BARC_MALE_22-30',
        'DF23': 'FB_BARC_MALE_31-40',
        'DF24': 'FB_BARC_MALE_41-50',
        'DF25': 'FB_BARC_MALE_51-60',
        'DF26': 'FB_BARC_MALE_61PLUS',
        'DF51': 'FB_FEMALE_13-17',
        'DF52': 'FB_FEMALE_18-24',
        'DF53': 'FB_FEMALE_25-34',
        'DF54': 'FB_FEMALE_35-44',
        'DF55': 'FB_FEMALE_45-54',
        'DF56': 'FB_FEMALE_55-64',
        'DF57': 'FB_FEMALE_65PLUS',
        'DF61': 'FB_FEMALE_TV_2-14',
        'DF62': 'FB_FEMALE_TV_15-21',
        'DF63': 'FB_FEMALE_TV_22-30',
        'DF64': 'FB_FEMALE_TV_31-40',
        'DF65': 'FB_FEMALE_TV_41-50',
        'DF66': 'FB_FEMALE_TV_51-60',
        'DF67': 'FB_FEMALE_TV_61PLUS',
        'DF71': 'FB_BARC_FEMALE_15-21',
        'DF72': 'FB_BARC_FEMALE_22-30',
        'DF73': 'FB_BARC_FEMALE_31-40',
        'DF74': 'FB_BARC_FEMALE_41-50',
        'DF75': 'FB_BARC_FEMALE_51-60',
        'DF76': 'FB_BARC_FEMALE_61PLUS',
        'DE01': 'EMAIL_MALE_13-17',
        'DE02': 'EMAIL_MALE_18-24',
        'DE03': 'EMAIL_MALE_25-34',
        'DE04': 'EMAIL_MALE_35-44',
        'DE05': 'EMAIL_MALE_45-54',
        'DE06': 'EMAIL_MALE_55-64',
        'DE07': 'EMAIL_MALE_65PLUS',
        'DE21': 'EMAIL_BARC_MALE_15-21',
        'DE22': 'EMAIL_BARC_MALE_22-30',
        'DE23': 'EMAIL_BARC_MALE_31-40',
        'DE24': 'EMAIL_BARC_MALE_41-50',
        'DE25': 'EMAIL_BARC_MALE_51-60',
        'DE26': 'EMAIL_BARC_MALE_61PLUS',
        'DE51': 'EMAIL_FEMALE_13-17',
        'DE52': 'EMAIL_FEMALE_18-24',
        'DE53': 'EMAIL_FEMALE_25-34',
        'DE54': 'EMAIL_FEMALE_35-44',
        'DE55': 'EMAIL_FEMALE_45-54',
        'DE56': 'EMAIL_FEMALE_55-64',
        'DE57': 'EMAIL_FEMALE_65PLUS',
        'DE71': 'EMAIL_BARC_FEMALE_15-21',
        'DE72': 'EMAIL_BARC_FEMALE_22-30',
        'DE73': 'EMAIL_BARC_FEMALE_31-40',
        'DE74': 'EMAIL_BARC_FEMALE_41-50',
        'DE75': 'EMAIL_BARC_FEMALE_51-60',
        'DE76': 'EMAIL_BARC_FEMALE_61PLUS',
        'DP01': 'PHONE_MALE_13-17',
        'DP02': 'PHONE_MALE_18-24',
        'DP03': 'PHONE_MALE_25-34',
        'DP04': 'PHONE_MALE_35-44',
        'DP05': 'PHONE_MALE_45-54',
        'DP06': 'PHONE_MALE_55-64',
        'DP07': 'PHONE_MALE_65PLUS',
        'DP11': 'PHONE_MALE_TV_2-14',
        'DP12': 'PHONE_MALE_TV_15-21',
        'DP13': 'PHONE_MALE_TV_22-30',
        'DP14': 'PHONE_MALE_TV_31-40',
        'DP15': 'PHONE_MALE_TV_41-50',
        'DP16': 'PHONE_MALE_TV_51-60',
        'DP17': 'PHONE_MALE_TV_61PLUS',
        'DP21': 'PHONE_BARC_MALE_15-21',
        'DP22': 'PHONE_BARC_MALE_22-30',
        'DP23': 'PHONE_BARC_MALE_31-40',
        'DP24': 'PHONE_BARC_MALE_41-50',
        'DP25': 'PHONE_BARC_MALE_51-60',
        'DP26': 'PHONE_BARC_MALE_61PLUS',
        'DP51': 'PHONE_FEMALE_13-17',
        'DP52': 'PHONE_FEMALE_18-24',
        'DP53': 'PHONE_FEMALE_25-34',
        'DP54': 'PHONE_FEMALE_35-44',
        'DP55': 'PHONE_FEMALE_45-54',
        'DP56': 'PHONE_FEMALE_55-64',
        'DP57': 'PHONE_FEMALE_65PLUS',
        'DP61': 'PHONE_FEMALE_TV_2-14',
        'DP62': 'PHONE_FEMALE_TV_15-21',
        'DP63': 'PHONE_FEMALE_TV_22-30',
        'DP64': 'PHONE_FEMALE_TV_31-40',
        'DP65': 'PHONE_FEMALE_TV_41-50',
        'DP66': 'PHONE_FEMALE_TV_51-60',
        'DP67': 'PHONE_FEMALE_TV_61PLUS',
        'DP71': 'PHONE_BARC_FEMALE_15-21',
        'DP72': 'PHONE_BARC_FEMALE_22-30',
        'DP73': 'PHONE_BARC_FEMALE_31-40',
        'DP74': 'PHONE_BARC_FEMALE_41-50',
        'DP75': 'PHONE_BARC_FEMALE_51-60',
        'DP76': 'PHONE_BARC_FEMALE_61PLUS',
        # model
        'DM01': 'FMD009V0051317HIGHSRMLDESTADS',
        'DM02': 'FMD009V0051317SRMLDESTADS',
        'DM03': 'FMD009V0051334HIGHSRMLDESTADS',
        'DM04': 'FMD009V0051334SRMLDESTADS',
        'DM05': 'FMD009V0051824HIGHSRMLDESTADS',
        'DM06': 'FMD009V0051824SRMLDESTADS',
        'DM07': 'FMD009V0052534HIGHSRMLDESTADS',
        'DM08': 'FMD009V0052534SRMLDESTADS',
        'DM09': 'FMD009V0053599HIGHSRMLDESTADS',
        'DM10': 'FMD009V0053599SRMLDESTADS',
        'DM11': 'MMD009V0051317HIGHSRMLDESTADS',
        'DM12': 'MMD009V0051317SRMLDESTADS',
        'DM13': 'MMD009V0051334HIGHSRMLDESTADS',
        'DM14': 'MMD009V0051334SRMLDESTADS',
        'DM15': 'MMD009V0051824HIGHSRMLDESTADS',
        'DM16': 'MMD009V0051824SRMLDESTADS',
        'DM17': 'MMD009V0052534HIGHSRMLDESTADS',
        'DM18': 'MMD009V0052534SRMLDESTADS',
        'DM19': 'MMD009V0053599HIGHSRMLDESTADS',
        'DM20': 'MMD009V0053599SRMLDESTADS',
    }
    res = []
    for item in lst:
        if item in demo_short_to_long:
            res.append(demo_short_to_long[item])
        else:
            res.append(item)
    return res


@F.udf(returnType=StringType())
def parse_preroll_segment(lst):
    if lst is None:
        return None
    if type(lst) == str:
        lst = lst.split(",")
    return extract_segments(convert_short_tag_to_log_tag(lst))


@F.udf(returnType=StringType())
def parse_wv_segments(segments):
    if segments is None:
        return None
    try:
        js = json.loads(segments)
    except:
        return None
    if type(js) == list:
        lst = js
    elif type(js) == dict:
        lst = js.get('data', [])
    else:
        return None
    return extract_segments(lst)


@F.udf(returnType=TimestampType())
def parse_timestamp(date: str, ts: str):
    return pd.Timestamp(date + ' ' + ts, tz='asia/kolkata')


def preprocess_playout(df):
    return df\
        .withColumn('break_start', parse_timestamp('Start Date', 'Start Time')) \
        .withColumn('break_end', parse_timestamp('End Date', 'End Time')) \
        .selectExpr(
            '`Content ID` as content_id',
            'trim(lower(`Playout ID`)) as playout_id',
            'trim(lower(Language)) as language',
            'trim(lower(Tenant)) as country',
            'explode(split(trim(lower(Platform)), "\\\\|")) as platform',
            'break_start',
            'break_end',
        )\
        .where('break_start is not null and break_end is not null') \
        .withColumn('break_start', F.expr('cast(break_start as long)')) \
        .withColumn('break_end', F.expr('cast(break_end as long)'))


@F.udf(returnType=StringType())
def languages_process(language):
    res = []
    if language:
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


def process_regular_cohorts_by_date(date, playout):
    print('process_regular_tags', date)
    # print('begin', datetime.now())
    final_output_path = f'{INVENTORY_SAMPLING_PATH}cd={date}/'
    success_path = f'{final_output_path}_SUCCESS'
    if s3.isfile(success_path):
        print('skip')
        return
    # split playout data according to if platform is null
    playout_with_platform = playout.where('platform != "na"').cache()
    playout_without_platform = playout.where('platform == "na"').drop('platform').cache()
    # load watch_video data with regular cohorts
    raw_wt = spark.sql(f'select * from {WV_TABLE} where cd = "{date}"') \
        .where(F.col('dw_p_id').substr(-1, 1).isin(['2', 'a', 'e', '8'])) \
        .where(F.col('content_id').isin(playout.toPandas().content_id.drop_duplicates().tolist())) \
        .withColumn('timestamp', F.expr('coalesce(cast(from_unixtime(CAST(ts_occurred_ms/1000 as BIGINT)) as timestamp), timestamp) as timestamp')) # timestamp has changed in HotstarX
    wt = raw_wt[['dw_d_id', 'content_id',
                 F.expr('lower(audio_language) as audio_language'),
                 F.expr('lower(language) as language'),
                 F.expr('lower(platform) as platform'),
                 F.expr('lower(country) as country'),
                 F.expr('lower(city) as city'),
                 F.expr('lower(state) as state'),
                 F.expr('cast(timestamp as long) as end'),
                 F.expr('cast(timestamp as double) - watch_time as start'),
                 parse_wv_segments('user_segments').alias('cohort'),
                 ]] \
        .withColumn('audio_language', languages_process('audio_language'))\
        .withColumn('language', F.coalesce('language', 'audio_language'))\
        .withColumn('language_tmp', F.rand()) \
        .withColumn('language_tmp', F.expr('if(language_tmp<0.5, "hindi", "english")')) \
        .withColumn('language', F.expr('if(language is null or language in ("", "unknown language", "na"), language_tmp, language)'))
    # .withColumn('language', F.coalesce('language', 'audio_language')) \
        # print(f'firetv data on {date}:')
    # wt.where('platform="firetv"').groupBy('language', 'platform', 'country').count().show(10, False)
    # load preroll data with regular cohorts
    preroll = spark.read.parquet(f'{PREROLL_INVENTORY_PATH}cd={date}') \
        .select('dw_d_id', 'user_segment').dropDuplicates(['dw_d_id']) \
        .select('dw_d_id', parse_preroll_segment('user_segment').alias('preroll_cohort'))
    # use wt data join preroll data in case there are no segments in wt users
    wt_with_cohort = wt.join(preroll, on='dw_d_id', how='left').withColumn('cohort', F.expr('if(cohort is null or cohort = "", preroll_cohort, cohort)'))
    wt_with_cohort.write.mode('overwrite').parquet(TMP_WATCHED_VIDEO_PATH)
    wt_with_cohort = spark.read.parquet(TMP_WATCHED_VIDEO_PATH)
    wt_with_platform = wt_with_cohort.join(playout_with_platform.hint('broadcast'), on=['content_id', 'language', 'platform', 'country'])
    wt_without_platform = wt_with_cohort.join(playout_without_platform.hint('broadcast'), on=['content_id', 'language', 'country'])[wt_with_platform.columns]
    # calculate inventory and reach for each cohort
    npar = 32
    res = wt_with_platform\
        .union(wt_without_platform) \
        .withColumn('ad_time', F.expr('least(end, break_end) - greatest(start, break_start)'))\
        .where('ad_time > 0') \
        .groupby('content_id', 'playout_id', 'language', 'platform', 'country', 'city', 'state', 'cohort') \
        .agg(
            F.expr('sum(ad_time) as ad_time'),
            F.expr('count(distinct dw_d_id) as reach')
        ).repartition(npar)
    res.write.mode('overwrite').parquet(final_output_path)
    print('end', date)


def check_if_focal_season(sport_season_name):
    if isinstance(sport_season_name, str):
        sport_season_name = sport_season_name.lower()
        for t in FOCAL_TOURNAMENTS_FOR_SAMPLING:
            if t in sport_season_name:  # sport_season_name is a super-string of tournament
                return True
    return False


# load new matches which have not been updated
def load_new_matches(cd):
    last_cd = get_last_cd(INVENTORY_SAMPLING_PATH, cd)
    print(last_cd)
    matches = spark.read.parquet(MATCH_CMS_PATH_TEMPL % cd) \
        .where(f'startdate > "{last_cd}" and startdate < "{cd}"').toPandas()
    print(matches)
    return matches[matches.sportsseasonname.map(check_if_focal_season)]


def latest_match_days(cd, n):
    matches = spark.read.parquet(MATCH_CMS_PATH_TEMPL % cd)
    latest_days = matches[['startdate']].distinct().toPandas().startdate.sort_values()
    return latest_days[latest_days < cd].tolist()[-n:]


@F.udf(returnType=StringType())
def concat(tags: set):
    return '|'.join(sorted(tags))


# output: {"A_58290825": "C_14_1", ...}
def load_segment_to_custom_cohort_mapping(cd: str) -> dict:
    res = {}
    for r in load_requests(cd, REQUESTS_PATH_TEMPL):
        for x in r.get(CUSTOM_AUDIENCE_COL, []):
            if 'segmentName' in x:
                res[x['segmentName']] = x['customCohort']
    return res


@F.udf(returnType=StringType())
def convert_to_custom_cohort(long_tags):
    long_tags.sort()
    short_tags = [segment_dict[i] for i in long_tags if i in segment_dict]
    return '|'.join(short_tags)


def process_custom_cohorts(cd):
    # load segment_to_custom_cohort_mapping from request
    if s3.isfile(f'{CUSTOM_COHORT_PATH}cd={cd}/_SUCCESS'):
        print('skip')
        return
    global segment_dict
    segment_dict = load_segment_to_custom_cohort_mapping(cd)
    print("segment_dict")
    print(segment_dict)
    # load existing segments and filter valid segments according to segment_to_custom_cohort_mapping
    segment_path_list1 = s3.glob('hotstar-ads-targeting-us-east-1-prod/adw/user-segment/ap_user_tag/cd*/hr*/segment*/')
    segment_path_list2 = s3.glob('hotstar-ads-targeting-us-east-1-prod/adw/user-segment/custom-audience/cd*/hr*/segment*/')
    segment_filter = lambda x: any(x.endswith(c) for c in segment_dict)
    segment_path_list = ['s3://' + x for x in segment_path_list1 + segment_path_list2 if segment_filter(x)]
    if len(segment_path_list) > 0:
        custom_cohort_df = spark.read.parquet(*segment_path_list)\
            .groupby('dw_d_id')\
            .agg(F.collect_set('tag_type').alias('segments')) \
            .withColumn('segments', convert_to_custom_cohort('segments'))
        last_five_matches_days = latest_match_days(cd, 5)
        valid_date_str = ','.join(f'"{x}"' for x in last_five_matches_days)
        raw_wt_df = spark.sql(f'select * from {DAU_TABLE} where cd in ({valid_date_str})')
        wt_df = raw_wt_df[['dw_d_id',
            F.expr('lower(cms_genre) like "%cricket%" as is_cricket'),
            F.expr('case when watch_time < 86400 then watch_time else 0 end as watch_time')
        ]]
        res = wt_df\
            .join(custom_cohort_df, on='dw_d_id', how='left')\
            .groupby('is_cricket', 'segments')\
            .agg(F.expr('sum(watch_time) as watch_time'), F.expr('count(distinct dw_d_id) as reach'))
    else:
        default_costom_cohort = pd.DataFrame([[True, '', 1.0, 1.0]], columns=['is_cricket', 'segments', 'watch_time', 'reach'])
        res = spark.createDataFrame(default_costom_cohort)
    res.repartition(1).write.mode('overwrite').parquet(f'{CUSTOM_COHORT_PATH}cd={cd}/')


def process_regular_cohorts(cd):
    matches = load_new_matches(cd)
    print(matches)
    if len(matches) > 0:
        for date in matches.startdate.drop_duplicates():
            content_ids = matches[matches.startdate == date].content_id.tolist()
            try:
                bucket_name = "hotstar-ads-data-external-us-east-1-prod"
                folder_path = f'run_log/blaze/prod/test/{date}/'
                file_list = get_s3_paths(bucket_name, folder_path)
                raw_playout = load_multiple_csv_file(spark, file_list)
                raw_playout = raw_playout.where(raw_playout['Start Date'].isNotNull() & raw_playout['End Date'].isNotNull())
                playout = preprocess_playout(raw_playout)\
                    .where(F.col('content_id').isin(content_ids))
                playout.toPandas()  # data checking, will fail if format of the playout is invalid
                process_regular_cohorts_by_date(date, playout)
            except Exception as e:
                print(date, 'error happend!!')
                print(e)


def main(cd):
    process_regular_cohorts(cd)
    process_custom_cohorts(cd)


if __name__ == '__main__':
    spark = hive_spark("etl")
    DATE = sys.argv[1]
    main(DATE)
