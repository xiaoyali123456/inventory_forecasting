import os
import sys

import gspread
import pandas as pd
import s3fs

input_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling_v2/inventory/'
config_path = 's3://hotstar-ads-data-internal-us-east-1-prod/data/workflows/adtech-segment-management/prod/ssai_configuration/ssai_configuration.json'
output_path = 's3://hotstar-ads-data-internal-us-east-1-prod/data/workflows/adtech-segment-management/prod/forecast_information/forecast_information.json'
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
            if x.startswith('FMD00') or '_FEMALE_' in x:
                return 'f'
            if x.startswith('MMD00') or '_MALE_' in x:
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


def load_history(cd):
    days = get_last_days(input_path, n=15)
    print(days)
    df = pd.concat([pd.read_parquet(input_path + 'cd=' + cd) for cd in days])
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


def main(cd):
    '''
    df: raw input
    df2: segment -> density
    df3: SSAI(after aggregation) -> density
    '''
    df = load_history(cd)
    df2, df3 = transform(df)
    df2.to_json(output_path, orient='records')
    df2['proportion'] = df2.reach / df2.reach.sum()
    df3['proportion'] = df3.reach / df3.reach.sum()
    df3.drop(columns='index', inplace=True)
    df2.to_json(output2_path + cd + '.json', orient='records')  # save for future check
    os.system('aws s3 cp s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/minliang.lin@hotstar.com-service-account.json my_service_account.json')
    gc = gspread.service_account('my_service_account.json')
    sheet = gc.open('ssai_cohort_density_forecast')
    sheet.sheet1.clear()
    sheet.sheet1.update([df2.columns.values.tolist()] + df2.values.tolist())
    sheet2 = sheet.get_worksheet(1)
    sheet2.clear()
    sheet2.update([df3.columns.values.tolist()] + df3.values.tolist())

    # check performance
    bl = load_baseline()
    bl2, _ = transform(bl)
    basic = ['age', 'device', 'gender', 'state', 'city', 'platform', 'nccs']
    cmp = df2.merge(bl2, on=basic, how='outer', suffixes=('', '_bl'))
    print(df2.groupby('density').size())
    print('total', len(df2))
    print(cmp[cmp.density != cmp.density_bl])
    print('predict reach', cmp.reach.sum())
    print('baseline reach', cmp.reach_bl.sum())
    print('predict NA reach%', cmp[cmp.reach.isna()].reach_bl.sum()/cmp.reach_bl.sum())
    print('baseline NA reach%', cmp[cmp.reach_bl.isna()].reach.sum()/cmp.reach.sum())
    print('mismatch reach%', cmp[cmp.density!=cmp.density_bl].reach.sum()/cmp.reach.sum())
    print('mismatch reach_baseline%', cmp[cmp.density!=cmp.density_bl].reach_bl.sum()/cmp.reach_bl.sum())


if __name__ == '__main__':
    cd = sys.argv[1]
    main(cd)
