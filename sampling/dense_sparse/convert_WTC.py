import pandas as pd

def classisfy(rank, thd1=0.02, thd2=0.5):
    if rank <= thd1:
        return 'super_dense'
    elif rank <= thd1 + thd2:
        return 'dense'
    else:
        return 'sparse'


INPUT_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/dense_sparse/qdata_v3/'
OUTPUT_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/dense_sparse/final/cd=2023-06-01/all.json'
SSAI_CONFIG_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/dense_sparse/ssai_configuration_v2.json'

df = pd.read_parquet(INPUT_PATH) # don't use spark due to limit of master result
df.reach *= 4
df.watch_time *= 4
df = df.loc[df.is_cricket == True]
ssai_config = pd.read_json(SSAI_CONFIG_PATH)

map = {
    'AgeTag': 'age',
    'AffluenceTag': 'device',
    'GenderTag': 'gender',
    'StateTags': 'state',
    'CityTags': 'city',
    'AsnTag': 'asn',
    'PlatformTag': 'platform',
    'NCCSAffluenceTag': 'nccs',
    'language': 'language',
    # 'CustomTags': 'custom',
}
inv = {v: k for k,v in map.items()}

ssai_config['basic'] = ssai_config.tagType.apply(lambda x: map.get(x, x))
ssai_config = ssai_config[ssai_config.enabled]

# basic = ['language', 'platform', 'city', 'state', 'nccs', 'device', 'gender', 'age']
basic = [x for x in ssai_config['basic'] if x in df.columns] + ['language']

df2 = df.groupby(basic).reach.sum().reset_index()
for i in basic:
    if inv[i] in ssai_config.tagType.tolist():
        row = ssai_config[ssai_config.tagType == inv[i]].iloc[0]
        meta = row.metaData
        if meta['type'] == 'map':
            # meta.pop('type')
            df2[i] = df2[i].map(lambda x: x if x in meta else 'other')
        elif meta['type'] == 'gender':
            pass
df2 = df2.groupby(basic).reach.sum().reset_index()

df2['rank'] = df2.reach.rank(method='first', ascending=False, pct=True)
df2['density'] = df2['rank'].map(lambda x: classisfy(x, 0.02, 0.5))

row = ssai_config[ssai_config.tagType == 'CustomTags'].iloc[0]
meta = row.metaData
custom_tags = list(set(f'C_{v["groupNumber"]}_{v["priority"]}' for k,v in meta['customCohort'].items() if v.get('enabled', True)))
# custom_tags.add(row.defaultValue)
custom_tag_df = pd.DataFrame({'custom_cohort': custom_tags})

# df3 = df2.drop(columns=['rank', 'reach'])
df3 = df2.drop(columns=['rank'])
# TODO: exclude below useless fields
df3['requestId'] = 1
df3['tournamentId'] = 1
df3['matchId'] = 1

df3['forecast_version'] = 'mlv1'
df4 = df3.merge(custom_tag_df, how='cross')
print(df4.iloc[:2].to_json(orient='records', indent=2))

# final
df4.to_json(OUTPUT_PATH, orient='records')
