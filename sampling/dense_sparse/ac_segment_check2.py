import os
import sys

import pandas as pd
import s3fs

input_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_v2/inventory/'
config_path = 's3://hotstar-ads-data-internal-us-east-1-prod/data/workflows/adtech-segment-management/prod/ssai_configuration/ssai_configuration.json'
output_path = 's3://hotstar-ads-data-internal-us-east-1-prod/data/workflows/adtech-segment-management/prod/forecast_information/100/forecast_information.json'
output2_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/cohort_density/forecast_information/'

s3 = s3fs.S3FileSystem()
age_mapping = {
    "EMAIL_FEMALE_13-17": "13-17",
    "EMAIL_FEMALE_18-24": "18-24",
    "EMAIL_FEMALE_25-34": "25-34",
    "EMAIL_FEMALE_35-44": "35-44",
    "EMAIL_FEMALE_45-54": "45-54",
    "EMAIL_FEMALE_55-64": "55-64",
    "EMAIL_FEMALE_65PLUS": "65PLUS",
    "EMAIL_MALE_13-17": "13-17",
    "EMAIL_MALE_18-24": "18-24",
    "EMAIL_MALE_25-34": "25-34",
    "EMAIL_MALE_35-44": "35-44",
    "EMAIL_MALE_45-54": "45-54",
    "EMAIL_MALE_55-64": "55-64",
    "EMAIL_MALE_65PLUS": "65PLUS",
    "FB_FEMALE_13-17": "13-17",
    "FB_FEMALE_18-24": "18-24",
    "FB_FEMALE_25-34": "25-34",
    "FB_FEMALE_35-44": "35-44",
    "FB_FEMALE_45-54": "45-54",
    "FB_FEMALE_55-64": "55-64",
    "FB_FEMALE_65PLUS": "65PLUS",
    "FB_MALE_13-17": "13-17",
    "FB_MALE_18-24": "18-24",
    "FB_MALE_25-34": "25-34",
    "FB_MALE_35-44": "35-44",
    "FB_MALE_45-54": "45-54",
    "FB_MALE_55-64": "55-64",
    "FB_MALE_65PLUS": "65PLUS",
    "PHONE_FEMALE_13-17": "13-17",
    "PHONE_FEMALE_18-24": "18-24",
    "PHONE_FEMALE_25-34": "25-34",
    "PHONE_FEMALE_35-44": "35-44",
    "PHONE_FEMALE_45-54": "45-54",
    "PHONE_FEMALE_55-64": "55-64",
    "PHONE_FEMALE_65PLUS": "65PLUS",
    "PHONE_MALE_13-17": "13-17",
    "PHONE_MALE_18-24": "18-24",
    "PHONE_MALE_25-34": "25-34",
    "PHONE_MALE_35-44": "35-44",
    "PHONE_MALE_45-54": "45-54",
    "PHONE_MALE_55-64": "55-64",
    "PHONE_MALE_65PLUS": "65PLUS",
    "FMD009V0051317HIGHSRMLDESTADS": "13-17",
    "FMD009V0051317SRMLDESTADS": "13-17",
    "FMD009V0051824HIGHSRMLDESTADS": "18-24",
    "FMD009V0051824SRMLDESTADS": "18-24",
    "FMD009V0052534HIGHSRMLDESTADS": "25-34",
    "FMD009V0052534SRMLDESTADS": "25-34",
    "FMD009V0053599HIGHSRMLDESTADS": "35-99",
    "FMD009V0053599SRMLDESTADS": "35-99",
    "MMD009V0051317HIGHSRMLDESTADS": "13-17",
    "MMD009V0051317SRMLDESTADS": "13-17",
    "MMD009V0051824HIGHSRMLDESTADS": "18-24",
    "MMD009V0051824SRMLDESTADS": "18-24",
    "MMD009V0052534HIGHSRMLDESTADS": "25-34",
    "MMD009V0052534SRMLDESTADS": "25-34",
    "MMD009V0053599HIGHSRMLDESTADS": "35-99",
    "MMD009V0053599SRMLDESTADS": "35-99",
    "R_F1317": "13-17",
    "R_F1824": "18-24",
    "R_F2534": "25-34",
    "R_F3599": "35-99",
    "R_M1317": "13-17",
    "R_M1824": "18-24",
    "R_M2534": "25-34",
    "R_M3599": "35-99",
}

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


def get_last_days(path, end=None, n=1):
    lst = [x.split('=')[-1] for x in s3.ls(path)]
    lst = sorted([x for x in lst if '$' not in x])
    if end is not None:
        lst = [x for x in lst if x < end]
    return lst[-n:]


def get_age(cohort):
    if cohort is not None:
        for x in cohort.split('|'):
            if x in age_mapping:
                return x
    return ''


def get_gender(cohort):
    if cohort is not None:
        for x in cohort.split('|'):
            if x.startswith('FMD00') or '_FEMALE_' in x or x.startswith('R_F'):
                return 'f'
            if x.startswith('MMD00') or '_MALE_' in x or x.startswith('R_M'):
                return 'm'
    return ''


def get_device(cohort):
    if cohort is not None:
        dc = {'A_15031263': '15-20K', 'A_94523754': '20-25K', 'A_40990869': '25-35K', 'A_21231588': '35K+'}
        for x in cohort.split('|'):
            if x in dc:
                return x
    return ''


def get_nccs(cohort):
    if cohort is not None:
        for x in cohort.split('|'):
            if x.startswith('NCCS_'):
                return x
    return ''


def load_history(cd, n=3):
    days = get_last_days(input_path, n=n)
    print(days)
    df = pd.concat([pd.read_parquet(input_path + 'cd=' + i) for i in days if i <= cd])
    df2 = df.groupby(['language', 'platform', 'city', 'state', 'cohort']).reach.sum().reset_index()
    df2['age'] = df2.cohort.map(get_age)
    df2['gender'] = df2.cohort.map(get_gender)
    df2['device'] = df2.cohort.map(get_device)
    df2['nccs'] = df2.cohort.map(get_nccs)
    df3 = df2.groupby(['language', 'platform', 'city', 'state', 'age', 'gender', 'device', 'nccs']).reach.sum().reset_index()
    return df3


def load_baseline():
    df = pd.read_parquet(input_path + 'cd=2022-11-10')
    df2 = df.groupby(['language', 'platform', 'city', 'state', 'cohort']).reach.sum().reset_index()
    df2['age'] = df2.cohort.map(get_age)
    df2['gender'] = df2.cohort.map(get_gender)
    df2['device'] = df2.cohort.map(get_device)
    df2['nccs'] = df2.cohort.map(get_nccs)
    df3 = df2.groupby(['language', 'platform', 'city', 'state', 'age', 'gender', 'device', 'nccs']).reach.sum().reset_index()
    return df3


def classify(rank, threshold1, threshold2):
    if rank <= threshold1:
        return 'super_dense'
    elif rank <= threshold1 + threshold2:
        return 'dense'
    else:
        return 'sparse'


def transform(df):
    forward = {
        'AgeTag': 'age',
        'AffluenceTag': 'device',
        'GenderTag': 'gender',
        'StateTags': 'state',
        'CityTags': 'city',
        'AsnTag': 'asn',
        'PlatformTag': 'platform',
        'NCCSAffluenceTag': 'nccs',
    }
    backward = {v: k for k, v in forward.items()}
    config = pd.read_json(config_path)
    config['basic'] = config.tagType.map(lambda x: forward.get(x, x))
    basic = [x for x in config['basic'] if x in df.columns]
    # cohorts that would be delivered, grouped by `key` of config
    df2 = df.groupby(basic).reach.sum().reset_index()
    for i in basic:
        if backward[i] in config.tagType.tolist():
            row = config[config.tagType == backward[i]].iloc[0]
            meta = row.metaData
            if meta['type'] == 'map':
                for x in row.acceptableValues:
                    meta[x] = x
                if i == 'age':
                    meta2 = {demo_short_to_long[k]: v for k, v in meta.items() if k in demo_short_to_long}
                    meta = {**meta, **meta2}
                    df2[i] = df2[i].map(lambda x: meta[x] if x in meta else row.defaultValue)
                else:
                    df2[i] = df2[i].map(lambda x: x if x in meta else row.defaultValue)
            elif meta['type'] == 'gender':
                df2[i] = df2[i].map(lambda x: x if x in ['f', 'm'] else row.defaultValue)
    df2 = df2.groupby(basic).reach.sum().reset_index()
    # simulate how these cohorts are treated in SSAI, grouped by `value` of config, num(`key`) > num(`value`)
    df3 = df2.reset_index()
    for i in basic:
        if backward[i] in config.tagType.tolist():
            row = config[config.tagType == backward[i]].iloc[0]
            meta = row.metaData
            if meta['type'] == 'map':
                for x in row.acceptableValues:
                    meta[x] = x
                df3[i] = df3[i].map(lambda x: meta[x] if x in meta else row.defaultValue)
            elif meta['type'] == 'gender':
                pass
    df3 = df3.groupby(basic).agg({'reach': sum, 'index': list}).reset_index()
    df3['rank'] = df3.reach.rank(method='first', ascending=False, pct=True)
    df3['density'] = df3['rank'].map(lambda x: classify(x, 0.02, 0.5))
    # apply df3 class to df2
    for k, v in df3.groupby('density').index.sum().items():
        df2.loc[v, 'density'] = k
    return df2, df3


old = load_history('2023-06-10', 3)
old2, old3 = transform(old)

new = load_history('2023-08-31', 1)
new2, new3 = transform(new)

cols = new3.columns[:7].tolist()
cmp = old3.merge(new3, on=cols, how='outer')
cmp[['density_x', 'density_y']] = cmp[['density_x', 'density_y']].fillna('NAN')
cmpg = cmp.groupby(['density_x', 'density_y']).agg({'reach_x': 'size', 'reach_x': 'sum', 'reach_y': 'sum'}).reset_index()
cmpg['rate_x'] = cmpg.reach_x / cmpg.reach_x.sum()
cmpg['rate_y'] = cmpg.reach_y / cmpg.reach_y.sum()

'''
             density_x    rate_x                                 rate_y
density_y                    NAN     dense    sparse super_dense    NAN     dense    sparse super_dense
0                  NAN       NaN  0.000000  0.000000         NaN    NaN  0.005761  0.000414         NaN
1                dense  0.001745  0.331580  0.011789    0.024256    0.0  0.272065  0.001105    0.058966
2               sparse  0.000760  0.000532  0.002941         NaN    0.0  0.004519  0.001945         NaN
3          super_dense       NaN  0.083354       NaN    0.543043    NaN  0.035543       NaN    0.619681
'''
