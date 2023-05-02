
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
    t = pd.concat([gt[top_cols], pr[top_cols]], axis=1)
    new_cols = t.columns.tolist()
    for i in range(len(new_cols) // 2, len(new_cols)):
        new_cols[i] += '_Pr'
    t.columns = new_cols
    print(t.reset_index())
