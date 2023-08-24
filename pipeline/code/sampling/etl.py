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


def make_segment_str(lst):
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


@F.udf(returnType=StringType())
def parse_preroll_segment(lst):
    if lst is None:
        return None
    if type(lst) == str:
        lst = lst.split(",")
    return make_segment_str(lst)


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
    return make_segment_str(lst)


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
    print('end', datetime.now())


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
    matches = spark.read.parquet(MATCH_CMS_PATH_TEMPL % cd) \
        .where(f'startdate > "{last_cd}"').toPandas()
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
    for date in matches.startdate.drop_duplicates():
        content_ids = matches[matches.startdate == date].content_id.tolist()
        try:
            raw_playout = spark.read.csv(PLAYOUT_PATH + date, header=True)
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
