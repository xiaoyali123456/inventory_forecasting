from datetime import datetime
import sys
from common import *
import pandas as pd
from pyspark.sql.types import TimestampType

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
    wt = spark.sql(f'select * from {WV_TABLE} where cd = "{dt}"')
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
    matches = spark.read.parquet(NEW_MATCHES_PATH_TEMPL % cd) \
        .where(f'startdate > "{last_cd}"').toPandas()
    return matches[matches.sportsseasonname.map(match_filter)] # TODO: exception check and rename this obscure name

def main(cd):
    matches = load_new_matches(cd)
    for dt in matches.startdate.drop_duplicates():
        content_ids = matches[matches.startdate == dt].content_id.tolist()
        playout = preprocess_playout(spark.read.csv(PLAYOUT_PATH + dt, header=True)) \
            .where(F.col('content_id').isin(content_ids))
        process(dt, playout)

if __name__ == '__main__':
    DATE = sys.argv[1]
    main(DATE)

