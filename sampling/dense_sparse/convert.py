import pandas as pd

INPUT_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/dense_sparse/qdata_v3/'
OUTPUT_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/dense_sparse/final/cd=2023-05-01/all.json'
OUTPUT_GZ_PATH = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/dense_sparse/final/cd=2023-05-02/all.json.gz'

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
custom_tags = pd.DataFrame({'custom_cohort': ['', 'C14_1', 'C15_1', 'C14_1|C15_1']})

df3 = df2.drop(columns='rank')
df3['requestId'] = 1
df3['tournamentId'] = 1
df3['matchId'] = 1
df3['forecast_version'] = 'mlv1'
df3['custom_cohort'] =  custom_tags
df4 = df3.merge(custom_tags, how='cross')
print(df4.iloc[:2].to_json(orient='records', indent=2))

# final
# df4.to_json(OUTPUT_PATH, orient='records')
df4.to_json(OUTPUT_GZ_PATH, orient='records')

