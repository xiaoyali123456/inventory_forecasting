#!/usr/bin/env python
# coding: utf-8

# ## Mask Out Experiment of DAU Prophet

# In[76]:


from prophet import Prophet
import pandas as pd


# In[171]:


holidays = pd.read_csv('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/holidays/latest/holidays_v2_4.csv')
df = spark.read.parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/DAU_full_v2/all/cd=2023-04-11/').toPandas()


# In[25]:


mask_period = {
    'wc2022': ['2022-10-16', '2022-11-13'],
    'wc2021': ['2021-10-17', '2021-11-14'],
    'ac2022': ['2022-08-27', '2022-09-11'],
}


# In[110]:


masked_df = {
    k: df[(df.ds > v[1])|(df.ds < v[0])] for k, v in mask_period.items()
}
gt = {
    k: df[(df.ds <= v[1])&(df.ds >= v[0])] for k, v in mask_period.items()
}


# In[116]:


def predict(df, future):
    m = Prophet(holidays=holidays)
    m.add_country_holidays(country_name='IN')
    m.fit(df)
    forecast = m.predict(future)
    return m, forecast


# In[ ]:


f_dc = {}
for k in masked_df:
    _, f_dc[k] = predict(masked_df[k].rename(columns={'vv': 'y'}), df[['ds']])


# In[164]:


def plot(k='wc2022'):
    x = masked_df[k]
    y = gt[k]
    df2 = df.copy()
    df2.loc[x.index, 'vv_train'] = df.loc[x.index].vv
    df2.loc[y.index, 'vv_gt'] = df.loc[y.index].vv
    df2['vv_hat'] = f_dc[k].yhat
    df2.plot(x='ds', y=['vv_train', 'vv_gt', 'vv_hat'], figsize=(30, 8), alpha=0.7, title=k, grid=True)


# In[165]:


for k in masked_df:
    plot(k)


# In[131]:


import pickle
with open('../data/forecast_mask_dc.pkl', 'wb') as fp:
    pickle.dump(f_dc, fp)


# In[132]:


get_ipython().system('aws s3 cp ../data/forecast_mask_dc.pkl s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/archive/DAU_forecast_mask/v1.pkl')


# In[150]:


def mape(k):
    f = f_dc[k].copy()
    f.ds = f.ds.map(lambda x: x.date().isoformat())
    merge = f.merge(gt[k], on='ds')
    return ((merge.yhat-merge.vv)/merge.vv).abs().mean()

for k in masked_df:
    print(k, mape(k))


# In[188]:


def mape2(k):
    f = f_dc[k].copy()
    f.ds = f.ds.map(lambda x: x.date().isoformat())
    merge = f.merge(gt[k], on='ds')
    return abs(merge.yhat.sum()-merge.vv.sum())/merge.vv.sum()

for k in masked_df:
    print(k, mape2(k))


# In[155]:


k = 'wc2022'
f = f_dc[k].copy()
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


# In[ ]:




