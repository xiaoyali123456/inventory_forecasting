import pandas as pd

def classify(rank, thd1=0.02, thd2=0.5):
    if rank <= thd1:
        return 'super_dense'
    elif rank <= thd1 + thd2:
        return 'dense'
    else:
        return 'sparse'


INPUT_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/dense_sparse/qdata_v3/'
OUTPUT_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/dense_sparse/final/cd=2023-05-27/all.json'
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
            df2[i] = df2[i].map(lambda x: x if x in meta else row.defaultValue)
        elif meta['type'] == 'gender':
            df2[i] = df2[i].map(lambda x: x if x in ['f', 'm'] else row.defaultValue)
df2 = df2.groupby(basic).reach.sum().reset_index()

df3 = df2.reset_index()
for i in basic:
    if inv[i] in ssai_config.tagType.tolist():
        row = ssai_config[ssai_config.tagType == inv[i]].iloc[0]
        meta = row.metaData
        if meta['type'] == 'map':
            df3[i] = df3[i].map(lambda x: meta[x] if x in meta else row.defaultValue)
        elif meta['type'] == 'gender':
            pass
df3 = df3.groupby(basic).agg({'reach':sum, 'index':list}).reset_index()
df3['rank'] = df3.reach.rank(method='first', ascending=False, pct=True)
df3['density'] = df3['rank'].map(lambda x: classify(x, 0.02, 0.5))

for k, v in df3.groupby('density').index.sum().items():
    df2.loc[v, 'density'] = k
df2.drop(columns=['rank', 'reach']).to_json(OUTPUT_PATH, orient='records')
