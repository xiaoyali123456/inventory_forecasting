"""
 1. generate table ('content_id', 'playout_id', 'language', 'platform', 'country', 'city', 'state', 'cohort', 'ad_time', 'reach')
 for regular cohorts distribution in terms of finished matches
    1.1. filter the matches that are not calculated and belongs to important tournaments
    1.2. load playout table and watch_video table
    1.3. parse user segments col in watch_video table to get the cohort info
    1.4. join these 2 tables to calculate ad_time and reach

 2. generate table ('is_cricket', 'segments', 'watch_time', 'reach') for custom cohorts distribution in terms of recent five days
    2.1. load segment-ssai mapping from request
    2.2. load user-segment table and convert segments to ssai tag
    2.3. load watch_video table for recent 5 days
    2.4. join these 2 tables to calculate watch_time and reach
    segments="C14_1|C15_2"
"""

from common import *
from datetime import datetime
import sys
import pandas as pd


def make_segment_str(lst):
    filtered = set()
    equals = ['A_15031263', 'A_94523754', 'A_40990869', 'A_21231588'] # device price
    prefixs = ['NCCS_', 'CITY_', 'STATE_', 'FMD00', 'MMD00', 'P_']
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
def make_segment_str_wrapper(lst):
    if lst is None:
        return None
    return make_segment_str(lst)

@F.udf(returnType=StringType())
def parse(segments):
    if segments is None:
        return None
    try:
        js = json.loads(segments)
    except:
        return None
    if type(js) == list:
        lst = js
    elif type(js) == dict:
        lst =js.get('data', [])
    else:
        return None
    return make_segment_str(lst)


@F.udf(returnType=TimestampType())
def parse_timestamp(date:str, ts: str):
    return pd.Timestamp(date + ' ' + ts, tz='asia/kolkata')


def preprocess_playout(df):
    # TODO: This will fail when End Date is null!! e.g. 2023-07-06
    return df.withColumn('break_start', parse_timestamp('Start Date', 'Start Time')) \
        .withColumn('break_end', parse_timestamp('End Date', 'End Time')) \
        .selectExpr(
            '`Content ID` as content_id',
            'trim(lower(`Playout ID`)) as playout_id',
            'trim(lower(Language)) as language',
            'trim(lower(Tenant)) as country',
            'explode(split(trim(lower(Platform)), "\\\\|")) as platform',
            'break_start',
            'break_end',
        ).where('break_start is not null and break_end is not null')


def process(dt, playout):
    print('process', dt)
    print('begin', datetime.now())
    final_output_path = f'{INVENTORY_SAMPLING_PATH}cd={dt}/'
    success_path = f'{final_output_path}_SUCCESS'
    if s3.isfile(success_path):
        print('skip')
        return
    playout1 = playout \
        .withColumn('break_start', F.expr('cast(break_start as long)')) \
        .withColumn('break_end', F.expr('cast(break_end as long)'))
    playout2 = playout1.where('platform != "na"')
    playout3 = playout2.where('platform == "na"').drop('platform')
    # TODO: verify dw_p_id difference with dw_d_id sampling
    wt = spark.sql(f'select * from {WV_TABLE} where cd = "{dt}"') \
        .where(F.col('dw_p_id').substr(-1, 1).isin(['2', 'a', 'e', '8'])) \
        .where(F.col('content_id').isin(playout.toPandas().content_id.drop_duplicates().tolist())) \
        .withColumn('timestamp', F.expr('coalesce(cast(from_unixtime(CAST(ts_occurred_ms/1000 as BIGINT)) as timestamp), timestamp) as timestamp')) # timestamp has changed in HotstarX
    # TODO: use received_at if received_at < timestamp
    wt1 = wt[['dw_d_id', 'content_id',
        F.expr('lower(language) as language'),
        F.expr('lower(platform) as platform'),
        F.expr('lower(country) as country'),
        F.expr('lower(city) as city'),
        F.expr('lower(state) as state'),
        F.expr('cast(timestamp as long) as end'),
        F.expr('cast(timestamp as double) - watch_time as start'),
        parse('user_segments').alias('cohort'),
    ]]
    preroll = spark.read.parquet(f'{PREROLL_INVENTORY_PATH}cd={dt}') \
        .select('dw_d_id', 'user_segment').dropDuplicates(['dw_d_id']) \
        .select('dw_d_id', make_segment_str_wrapper('user_segment').alias('preroll_cohort'))
    wt1 = wt1.join(preroll, on='dw_d_id', how='left').withColumn('cohort', F.coalesce('cohort', 'preroll_cohort'))
    wt1.write.mode('overwrite').parquet(TMP_WATCHED_VIDEO_PATH)
    wt1 = spark.read.parquet(TMP_WATCHED_VIDEO_PATH)
    wt2a = wt1.join(playout2.hint('broadcast'), on=['content_id', 'language', 'platform', 'country'])
    wt2b = wt1.join(playout3.hint('broadcast'), on=['content_id', 'language', 'country'])[wt2a.columns]
    wt2 = wt2a.union(wt2b)
    wt3 = wt2.withColumn('ad_time', F.expr('least(end, break_end) - greatest(start, break_start)'))
    npar = 32
    wt4 = wt3.where('ad_time > 0') \
        .groupby('content_id', 'playout_id',
            'language', 'platform', 'country',
            'city', 'state', 'cohort') \
        .agg(
            F.expr('sum(ad_time) as ad_time'),
            F.expr('count(distinct dw_d_id) as reach')
        ).repartition(npar)
    wt4.write.mode('overwrite').parquet(final_output_path)
    print('end', datetime.now())


def match_filter(s):
    if isinstance(s, str):
        s = s.lower()
        for t in FOCAL_TOURNAMENTS:
            if t in s: # sportseasonname is a superstring
                return True
    return False


def load_new_matches(cd):
    last_cd = get_last_cd(INVENTORY_SAMPLING_PATH, cd)
    matches = spark.read.parquet(MATCH_CMS_PATH_TEMPL % cd) \
        .where(f'startdate > "{last_cd}"').toPandas()
    return matches[matches.sportsseasonname.map(match_filter)] # TODO: exception check and rename this obscure name


def latest_match_days(cd, n):
    matches = spark.read.parquet(MATCH_CMS_PATH_TEMPL % cd)
    latest_days = matches[['startdate']].distinct().toPandas().startdate.sort_values()
    return latest_days[latest_days < cd].tolist()[-n:]


@F.udf(returnType=StringType())
def concat(tags: set):
    return '|'.join(sorted(tags))


def load_custom_tags(cd: str) -> dict:
    res = {}
    for r in load_requests(cd):
        for x in r.get('customAudiences', []):
            if 'segmentName' in x:
                res[x['segmentName']] = x['customCohort']
    return res


@F.udf(returnType=StringType())
def convert_custom_cohort(long_tags):
    long_tags.sort()
    short_tags = [c_tag_dict[i] for i in long_tags if i in c_tag_dict]  # Need drop duplicates?
    return '|'.join(short_tags)


def process_custom_tags(cd):
    # debug: 
    # c_tags=['A_58290825']
    global c_tag_dict
    c_tag_dict = load_custom_tags(cd)
    t = s3.glob('hotstar-ads-targeting-us-east-1-prod/adw/user-segment/ap_user_tag/cd*/hr*/segment*/')
    t2 = s3.glob('hotstar-ads-targeting-us-east-1-prod/adw/user-segment/custom-audience/cd*/hr*/segment*/')
    f = lambda x: any(x.endswith(c) for c in c_tag_dict)
    t3 = ['s3://' + x for x in t + t2 if f(x)]
    if len(t3) > 0:
        ct = spark.read.parquet(*t3).groupby('dw_d_id').agg(F.collect_set('tag_type').alias('segments')) \
            .withColumn('segments', convert_custom_cohort('segments'))
        matches_days = latest_match_days(cd, 5) # TODO: filter 3 month expiration
        sql = ','.join(f'"{x}"' for x in matches_days)
        wt = spark.sql(f'select * from {DAU_TABLE} where cd in ({sql})')
        wt1 = wt[['dw_d_id',
            F.expr('lower(cms_genre) == "cricket" as is_cricket'),
            F.expr('case when watch_time < 86400 then watch_time else 0 end as watch_time')
        ]]
        res = wt1.join(ct, on='dw_d_id', how='left').groupby('is_cricket', 'segments').agg(
            F.expr('sum(watch_time) as watch_time'),
            F.expr('count(distinct dw_d_id) as reach')
        )
    else:
        t = pd.DataFrame([[False, '', 1.0, 1.0]], columns=['is_cricket', 'segments', 'watch_time', 'reach'])
        res = spark.createDataFrame(t)
    res.repartition(1).write.mode('overwrite').parquet(f'{CUSTOM_COHORT_PATH}cd={cd}/')


def main(cd):
    matches = load_new_matches(cd)
    for dt in matches.startdate.drop_duplicates():
        content_ids = matches[matches.startdate == dt].content_id.tolist()
        try:
            raw_playout = spark.read.csv(PLAYOUT_PATH + dt, header=True) \
                .where(raw_playout['Start Date'].isNotNull() & raw_playout['End Date'].isNotNull()) # TODO: this doesn't cover all corner cases
            playout = preprocess_playout(raw_playout).where(F.col('content_id').isin(content_ids))
            playout.toPandas() # try to realize the playout
            process(dt, playout)
        except:
            print(dt, 'playout not available')
    process_custom_tags(cd)


if __name__ == '__main__':
    DATE = sys.argv[1]
    main(DATE)
