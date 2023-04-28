import pandas as pd

INPUT_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/dense_sparse/qdata_v2/'
OUTPUT_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/dense_sparse/final/cd=2023-04-28/all.json'

df = pd.read_parquet(INPUT_PATH) # don't use spark due to limit of master result
df.reach *= 4
df.watch_time *= 4
df = df.loc[df.is_cricket == True]

ssai_cfg = pd.read_json('data/ssai_configuration_v2.json')

def classisfy(rank, thd1=0.05, thd2=0.2):
    if rank <= thd1:
        return 'super_dense'
    elif rank <= thd1 + thd2:
        return 'dense'
    else:
        return 'sparse'

basic = ['platform', 'language', 'city', 'state', 'age', 'device', 'gender']
df2 = df.groupby(basic).reach.sum().reset_index()
df2['rank'] = df2.reach.rank(method='first', ascending=False, pct=True)
df2['density'] = df2['rank'].map(lambda x: classisfy(x, 0.02, 0.2))

convert = {
    'age': {
        'other': 'U30',
        '30+': '30+'
    },
    'device': {
        '':'A_21231588,
        '':'A_15031263,
        '':'A_40990869,
        '':'A_94523754,
    }
}

custom_tags = [['', 'C14_1'], ['', 'C15_1']]

for k, v in name_map.items():
    print(k, v)
    print(ssai_cfg[ssai_cfg.tagType == v].iloc[0].metaData)
    break

df3 = df2.drop(columns='rank')
df3['requestId'] = 1
df3['tournamentId'] = 1
df3['matchId'] = 1
df3['forecast_version'] = 'mlv1'
print(df3[:2].to_json(orient='records', indent=2))

# final
df3.to_json('test.json', orient='records')

