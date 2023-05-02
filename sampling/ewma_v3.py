# To investigate why the total ewma error is smaller than the expected.
import pandas as pd
import numpy as np
from tqdm import tqdm

time_cols = ['cd']
lambda_ = 0.8

# df = pd.read_parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/tmp/piv.parquet')
df2 = pd.read_parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/tmp/df2.parquet')

cohort_cols = ['country', 'language', 'platform', 'city', 'state', 'nccs', 'device', 'gender', 'age']
df3 = df2.pivot_table(index=time_cols, columns=cohort_cols, values='ad_time_ratio').fillna(0)
fun = np.frompyfunc(lambda x,y: lambda_ * x + (1-lambda_) * y, 2, 1) # x is the sum
df4 = pd.concat([fun.accumulate(df3[x], dtype=object) for x in tqdm(df3.columns)], axis=1).shift(1)
df4.columns.names = cohort_cols

# df3.sort_values(df3.index[-1], axis=1).iloc[:, -1] # view the largest column
invy = df2.groupby(time_cols)['ad_time'].sum()
raw_err = (df4 - df3).mul(invy, axis=0)

tail = lambda x: x[15:].sum().sum() # 10 -> 15, less error
print('raw_err', tail(raw_err.abs()) / tail(invy)) # 34%

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

# print detail
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
