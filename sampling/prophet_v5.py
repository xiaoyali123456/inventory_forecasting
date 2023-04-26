#!/usr/bin/env python
# coding: utf-8

# # Mask Out Experiment of DAU Prophet

# In[1]:


from prophet import Prophet
import pandas as pd


# In[2]:


holidays = pd.read_csv('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/holidays/latest/holidays_v2_4.csv')
df = spark.read.parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_full_v2/all/cd=2023-04-11/').toPandas()


# ## Mask Recent Big Tournament

# In[3]:


mask_period = {
    'wc2022': ['2022-10-16', '2022-11-13'],
    'wc2021': ['2021-10-17', '2021-11-14'],
    'ac2022': ['2022-08-27', '2022-09-11'],
}


# In[4]:


masked_df = {
    k: df[(df.ds > v[1])|(df.ds < v[0])] for k, v in mask_period.items()
}
gt = {
    k: df[(df.ds <= v[1])&(df.ds >= v[0])] for k, v in mask_period.items()
}


# In[5]:


def predict(df, future):
    m = Prophet(holidays=holidays)
    m.add_country_holidays(country_name='IN')
    m.fit(df)
    forecast = m.predict(future)
    return m, forecast


# In[ ]:


f_vv_dc = {}
for k in masked_df:
    _, f_vv_dc[k] = predict(masked_df[k].rename(columns={'vv': 'y'}), df[['ds']])

f_sub_vv_dc = {}
for k in masked_df:
    _, f_sub_vv_dc[k] = predict(masked_df[k].rename(columns={'sub_vv': 'y'}), df[['ds']])

# In[164]:

def plot(k='wc2022'):
    x = masked_df[k]
    y = gt[k]
    df2 = df.copy()
    df2.loc[x.index, 'vv_train'] = df.loc[x.index].vv
    df2.loc[y.index, 'vv_gt'] = df.loc[y.index].vv
    df2['vv_hat'] = f_vv_dc[k].yhat
    df2.plot(x='ds', y=['vv_train', 'vv_gt', 'vv_hat'], figsize=(30, 8), alpha=0.7, title=k, grid=True)


# In[165]:


for k in masked_df:
    plot(k)


# In[131]:


import pickle
all_dc = {'f_vv': f_vv_dc, 'f_sub_vv': f_sub_vv_dc}
with open('v2.pkl', 'wb') as fp:
    pickle.dump(all_dc, fp)


# In[132]:


get_ipython().system('aws s3 cp v2.pkl s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/archive/DAU_forecast_mask/v2.pkl')


# In[150]:


def mape(k):
    f = f_vv_dc[k].copy()
    f.ds = f.ds.map(lambda x: x.date().isoformat())
    merge = f.merge(gt[k], on='ds')
    return ((merge.yhat-merge.vv)/merge.vv).abs().mean()

for k in masked_df:
    print(k, mape(k))


# In[188]:


def mape2(k):
    f = f_vv_dc[k].copy()
    f.ds = f.ds.map(lambda x: x.date().isoformat())
    merge = f.merge(gt[k], on='ds')
    return abs(merge.yhat.sum()-merge.vv.sum())/merge.vv.sum()

for k in masked_df:
    print(k, mape2(k))


# In[155]:


k = 'wc2022'
f = f_vv_dc[k].copy()
f.ds = f.ds.map(lambda x: x.date().isoformat())
merge = f.merge(gt[k], on='ds')
pd.concat([merge[['ds', 'yhat', 'vv']], (merge.yhat-merge.vv)/merge.vv], axis=1)


# ## Sub DAU Analysis

# In[ ]:


holidays = pd.read_csv('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/holidays/latest/holidays_v2_4.csv')
df = spark.read.parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_full_v2/all/cd=2023-04-11/').toPandas()


# In[180]:


def predict(df):
    m = Prophet(holidays=holidays)
    m.add_country_holidays(country_name='IN')
    m.fit(df)
    future = m.make_future_dataframe(periods=365)
    forecast = m.predict(future)
    return m, forecast


# In[181]:


m, f = predict(df.rename(columns={'sub_vv': 'y'}))


# In[183]:


m.plot(f, figsize=(20, 8));


# In[185]:


m1, f1 = predict(df.rename(columns={'vv': 'y'}))
m1.plot(f1, figsize=(20, 8));


# In[ ]:


def predict(df, future):
    m = Prophet(holidays=holidays)
    m.add_country_holidays(country_name='IN')
    m.fit(df)
    forecast = m.predict(future)
    return m, forecast

f_dc2 = {}
for k in masked_df:
    _, f_dc2[k] = predict(masked_df[k].rename(columns={'sub_vv': 'y'}), df[['ds']])


# In[194]:


def plot2(k):
    x = masked_df[k]
    y = gt[k]
    df2 = df.copy()
    df2.loc[x.index, 'sub_vv_train'] = df.loc[x.index].sub_vv
    df2.loc[y.index, 'sub_vv_gt'] = df.loc[y.index].sub_vv
    df2['sub_vv_hat'] = f_dc2[k].yhat
    df2.plot(x='ds', y=['sub_vv_train', 'sub_vv_gt', 'sub_vv_hat'], figsize=(30, 8), alpha=0.7, title=k, grid=True)
for k in masked_df:
    plot2(k)


# In[195]:


def mape(k):
    f = f_dc2[k].copy()
    f.ds = f.ds.map(lambda x: x.date().isoformat())
    merge = f.merge(gt[k], on='ds')
    return ((merge.yhat-merge.sub_vv)/merge.sub_vv).abs().mean()

for k in masked_df:
    print(k, mape(k))


# ## Original Error

# In[6]:


m, f = predict(df.rename(columns={'vv': 'y'}), df[['ds']])


# In[16]:


def mape(k):
    f2 = f.copy()
    f2.ds = f2.ds.map(lambda x: x.date().isoformat())
    merge = f2.merge(gt[k], on='ds')
    err = ((merge.yhat - merge.vv)/merge.vv).rename('err%')
    print(pd.concat([merge[['yhat', 'vv']], err], axis=1))
    return err.abs().mean()

for k in masked_df:
    print(k, mape(k))


# In[14]:


(8+11+10)/3 # original MAPE


# In[13]:


(14+19+11)/3 # MAPE on mask


# # Add Regression Target

# In[17]:


def predict(df, future):
    m = Prophet(holidays=holidays)
    m.add_country_holidays(country_name='IN')
    m.fit(df)
    forecast = m.predict(future)
    return m, forecast


# In[37]:


match_days = holidays[holidays.holiday.isin(['t20','odi','test'])].ds.drop_duplicates().map(lambda x: str(x.date()))
match_days


# In[38]:


df.ds.isin(match_days).astype(int).sum() # smaller than expected


# In[39]:


df['is_match_day'] = df.ds.isin(match_days).astype(int)


# In[56]:


def predict(df, future):
    m = Prophet(holidays=holidays)
    m.add_country_holidays(country_name='IN')
    # m.add_regressor('is_match_day')
    m.fit(df)
    forecast = m.predict(future)
    return m, forecast

m, f= predict(df.rename(columns={'sub_vv':'y'}), df[['ds', 'is_match_day']])


# In[54]:


m.plot(f);


# In[53]:


m.plot_components(f);


# In[ ]:
import pickle
import os
pkl = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/archive/DAU_forecast_mask/v1.pkl'
os.system(f'aws s3 cp {pkl} /tmp')
with open('/tmp/v2.pkl', 'rb') as fp:
    all_dc = pickle.load(fp)

for mask in all_dc['f_vv']:
    f = all_dc['f_vv'][mask]
    pd.concat([f.ds.rename('cd'), f.yhat.rename('DAU'), all_dc['f_sub_vv'][mask].yhat.rename('subs_DAU')], axis=1) \
        .to_parquet(f's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_full_v2/masked/mask={mask}/cd=2023-04-11/p0.parquet')

test = pd.read_parquet(f's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_full_v2/masked/mask={mask}/cd=2023-04-11/p0.parquet')

