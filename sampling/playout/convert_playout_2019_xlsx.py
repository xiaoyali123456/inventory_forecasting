from glob import glob
import pandas as pd
import datetime

def conv(s):
    if isinstance(s, datetime.time):
        return (s.hour * 60 + s.minute)*60+s.second
    elif isinstance(s, int):
        return s
    else:
        return None

for i in glob('*.xlsx'): # */*.xlsx
   df = pd.read_excel(i, engine='openpyxl', skiprows=2)
   new_cols = [s.lower().replace(' ', '_') for s in df.columns]
   new_cols[3:5] = 'file_name', 'file_id'
   df.columns = new_cols
   df = df[~df.duration.isna()]
   language = i.split('_')[4] # or [3] or [5]
   df['language'] = language
   df['duration'] = df.duration.apply(conv)
   df[['id', 'date', 'time', 'file_name', 'file_id', 'time_in', 'duration', 'language']].to_csv(i.split('.')[0] + '.csv', index=False)

# match meta
match_meta='s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/ads_crash/match_meta'
mm = spark.read.parquet(match_meta)
mf = mm.where('startdate >= 1578960000 and startdate <= 1579219200').select('startdate', 'enddate', 'broadcastdate', 'title', 'premium', 's_title', 'contentid').toPandas() # Ind vs Aus 2020
mf = mm.where('startdate >= 1634428800 and startdate <= 1634515200').select('startdate', 'enddate', 'broadcastdate', 'title', 'premium', 's_title', 'contentid').toPandas() # T20 WC 2021

# England tour
from os import path
import os
import pandas as pd
import datetime

def trans2sec(s):
    if isinstance(s, datetime.time):
        return (s.hour * 60 + s.minute)*60+s.second
    elif isinstance(s, int):
        return s
    else:
        return None

def func(lst, skip=2, lang_pos=6):
    for f in lst:
        y=path.basename(f)
        out_dir=f'all/cd={y[4:8]}-{y[2:4]}-{y[:2]}/'
        os.makedirs(out_dir, exist_ok=True)
        # open
        df = pd.read_excel(f, engine='openpyxl', skiprows=skip)
        new_cols = [s.lower().replace(' ', '_') for s in df.columns]
        new_cols[3:5] = 'file_name', 'file_id'
        df.columns = new_cols
        df = df[~df.duration.isna()]
        language = f.split('_')[lang_pos].split('-')[0]
        df['language'] = language
        df['duration'] = df.duration.apply(trans2sec)
        break
        out_f = out_dir + path.basename(f.split('.')[0]) + '.csv'
        df[['id', 'date', 'time', 'file_name', 'file_id', 'time_in', 'duration', 'language']].to_csv(out_f, index=False)

# England Tour
# x=glob('*/*/India/*.xlsx') # ODI, T20
# x=glob('*/*/*/India/*.xlsx') # Test Series
# skip = 2
# lang_pos = 6

# IPL 2021 P1
func(glob('[NO]*/*/India/*.xlsx'), 2, 2)

#! aws s3 sync . s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/playout_original/
# check data
df=spark.read.csv('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/playout_original/',header=True)
pdf=df.toPandas()
pdf['dur']=pdf.duration.apply(int)
print('check duration range', pdf.dur.describe())
pdf.groupby('date')
print('check cross day match', pdf[pdf.date != pdf.cd.map(str)])

