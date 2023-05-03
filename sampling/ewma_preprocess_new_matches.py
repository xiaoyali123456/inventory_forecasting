import pandas as pd

DATE='2023-05-01'
iv = load_inventory(DATE)
piv = augment(iv, ['cd', 'content_id'], DATE)
piv.to_parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/tmp/piv_v2.parquet')
cohort_cols = ['country', 'language', 'platform', 'city', 'state', 'nccs', 'device', 'gender', 'age']
df2 = piv.fillna('')
group_cols=['cd']
target = 'ad_time'
df2[target+'_ratio'] = df2[target] / df2.groupby(group_cols)[target].transform('sum')
df2.to_parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/tmp/df2_v2.parquet')

# In[1]: new block
df2 = pd.read_parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/tmp/df2_v2.parquet')
df3 = df2.pivot_table(index=group_cols, columns=cohort_cols, values=target+'_ratio', aggfunc='sum').fillna(0)
df4 = df3.ewm(alpha=0.2, adjust=False).mean().shift(1, fill_value=1/df3.shape[1]) # uniform prior


tail = lambda x: x[29:].sum().sum()
print('raw_err', tail(raw_err.abs()) / tail(invy))

# print error
for attention in cohort_cols:
    gt = df3.groupby(level=attention, axis=1).sum()
    pr = df4.groupby(level=attention, axis=1).sum()
    err = pr - gt
    invy_err = err.mul(invy, axis=0)
    print(attention,
        'inventory', tail(invy),
        'err', tail(invy_err.abs())/tail(invy),
        'sign_err', tail(invy_err))

for attention in cohort_cols:
    gt = df3.groupby(level=attention, axis=1).sum()
    pr = df4.groupby(level=attention, axis=1).sum()
    top_cols = gt.sum().nlargest(10).index
    t = pd.concat([gt[top_cols], pr[top_cols].add_suffix('_Pr')], axis=1)
    print(t.reset_index())
