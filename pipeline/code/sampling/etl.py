from datetime import datetime, timedelta
import sys
import pandas as pd
from common import *

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

@F.udf(returnType=TimestampType())
def parseTimestamp(date:str, ts: str):
    return pd.Timestamp(date + ' ' + ts, tz='asia/kolkata')

def preprocess_playout(df):
    return df.withColumn('break_start', parseTimestamp('Start Date', 'Start Time')) \
        .withColumn('break_end', parseTimestamp('End Date', 'End Time')) \
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
    wt = spark.sql(f'select * from {WV_TABLE} where cd = "{dt}"') \
        .where(F.col('dw_p_id').substr(-1, 1).isin(['2', 'a', 'e', '8']))
    # TODO: use received_at if received_at < timestamp
    wt1 = wt[['dw_d_id', 'content_id', 'user_segments',
        F.expr('lower(language) as language'),
        F.expr('lower(platform) as platform'),
        F.expr('lower(country) as country'),
        F.expr('lower(city) as city'),
        F.expr('lower(state) as state'),
        F.expr('cast(timestamp as long) as end'),
        F.expr('cast(timestamp as double) - watch_time as start'),
    ]]
    wt2a = wt1.join(playout2.hint('broadcast'), on=['content_id', 'language', 'platform', 'country'])
    wt2b = wt1.join(playout3.hint('broadcast'), on=['content_id', 'language', 'country'])[wt2a.columns]
    wt2 = wt2a.union(wt2b)
    wt3 = wt2.withColumn('ad_time', F.expr('least(end, break_end) - greatest(start, break_start)'))
    npar = 32
    wt4 = wt3.where('ad_time > 0') \
        .withColumn('cohort', parse('user_segments')) \
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
            if t in s:
                return True
    return False

def load_new_matches(cd):
    last_cd = get_last_cd(INVENTORY_SAMPLING_PATH, cd)
    # last_cd = '2022-12-01' # debug
    matches = spark.read.parquet(NEW_MATCHES_PATH_TEMPL % cd) \
        .where(f'startdate > "{last_cd}"').toPandas()
    return matches[matches.sportsseasonname.map(match_filter)] # TODO: exception check and rename this obscure name

def load_custom_tags(cd):
    reqs = load_requests(cd)
    c_tags  = set()
    for r in reqs:
        for x in r.get('customAudiences', []):
            if 'segmentName' in x:
                c_tags.add(x['segmentName'])
    return list(c_tags)

@F.udf(returnType=StringType())
def concat(tags: set):
    return '|'.join(sorted(tags))

def custom_tags(cd):
    c_tags = load_custom_tags(cd)
    t = s3.glob('hotstar-ads-targeting-us-east-1-prod/adw/user-segment/ap_user_tag/cd*/hr*/segment*/')
    t2 = s3.glob('hotstar-ads-targeting-us-east-1-prod/adw/user-segment/custom-audience/cd*/hr*/segment*/')
    f = lambda x: any(x.endswith(c) for c in c_tags)
    t3 = ['s3://' + x for x in t + t2 if f(x)]
    ct = spark.read.parquet(*t3).groupby('dw_d_id').agg(F.collect_set('tag_type').alias('segments')) \
        .withColumn('segments', concat('segments'))
    yesterday = (datetime.strptime(cd, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
    wt = spark.sql(f'select * from {WV_TABLE} where cd = "{yesterday}"') \
        .where(F.col('dw_p_id').substr(-1, 1).isin(['2', 'a', 'e', '8']))
    wt1 = wt[['dw_d_id',
        F.expr('lower(genre) == "cricket" as is_cricket'),
        F.expr('case when watch_time < 86400 then watch_time else 0 end as watch_time')
    ]]
    wt1.join(ct, on='dw_d_id', how='left').groupby('is_cricket', 'segments').agg(
        F.expr('sum(watch_time) as watch_time'),
        F.expr('count(distinct dw_d_id) as reach')
    ).repartition(1).write.parquet(f'{CUSTOM_COHORT_PATH}cd={cd}/')

def main(cd):
    # cd = '2023-05-01' # TODO: fix customAudiences typo
    matches = load_new_matches(cd)
    for dt in matches.startdate.drop_duplicates():
        content_ids = matches[matches.startdate == dt].content_id.tolist()
        try:
            raw_playout = spark.read.csv(PLAYOUT_PATH + dt, header=True)
            playout = preprocess_playout(raw_playout).where(F.col('content_id').isin(content_ids))
            process(dt, playout)
        except:
            print(dt, 'playout not available')
    custom_tags(cd)

if __name__ == '__main__':
    DATE = sys.argv[1]
    main(DATE)

