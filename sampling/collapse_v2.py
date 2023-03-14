# !aws s3 sync s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/watched_time_for_collapse/quarter_data_v2/ quarter_data_v2\

import pandas as pd
import numpy as np

path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/watched_time_for_collapse/quarter_data_v2/'
basic = ['platform', 'device', 'language', 'city', 'state', 'age', 'gender']
df = pd.read_parquet('quarter_data_v2')

translation = {
    'city': {
        'mumbai':'mumbai',
        'delhi':'delhi',
        'bangalore':'bangalore',
        'hyderabad':'hyderabad',
        'ahmedabad':'ahmedabad',
        'chennai':'chennai',
        'kolkata':'kolkata',
        'surat':'surat',
        'pune':'pune',
        'jaipur':'jaipur',
        'lucknow':'lucknow',
        'kanpur':'kanpur',
        'nagpur':'nagpur',
        'indore':'indore',
        'bhopal':'bhopal',
        'visakhapatnam':'visakhapatnam',
        'patna':'patna',
        'vadodara':'vadodara',
        'ludhiana':'ludhiana',
        'agra':'agra',
        'nashik':'nashik',
        'varanasi':'varanasi',
        'dhanbad':'dhanbad',
        'amritsar':'amritsar',
        'allahabad':'allahabad',
        'ranchi':'ranchi',
        'gwalior':'gwalior',
        'coimbatore':'coimbatore',
        'vijayawada':'vijayawada',
        'jodhpur':'jodhpur',
        'madurai':'madurai',
        'raipur':'raipur',
        'kochi':'kochi',
        'chandigarh':'chandigarh',
        'guwahati':'guwahati',
        'other': 'other',
    },
    'device': {
        '25-35K': '25K+',
        '35K+': '25K+',
        'other': 'other'
    },
    'age': { # TODO: need check the definition of age
        'U30': '30-',
        'other': 'other',
    },
    'gender': {
        'm': 'male',
        'other': 'female',
    },
    'platform': {
        'android': 'mobile',
        'ios': 'mobile',
        'other': None,
    },
    'language': {
        'english': 'english',
        'hindi': 'hindi',
        'other': None,
    },
    'state': {
        'up': 'Uttar Pradesh + Uttarakhand',
        'ut': 'Uttar Pradesh + Uttarakhand',
        'mh': 'Maharashtra',
        'br': 'Bihar + Jharkhand',
        'jh': 'Bihar + Jharkhand',
        'wb': 'West Bengal',
        'ct': 'Chattisgarh',
        'tn': 'Tamil Nadu + Pondicherry',
        'ka': 'Karnataka',
        'gj': 'Gujarat + Goa',
        'ap': 'Andhra Pradesh + Telangana',
        'tg': 'Andhra Pradesh + Telangana',
        'or': 'Odisha',
        'kl': 'Kerala',
        'pb': 'Haryana + Punjab + Himachal Pradesh + Jammu and Kashmir',
        'hr': 'Haryana + Punjab + Himachal Pradesh + Jammu and Kashmir',
        'hp': 'Haryana + Punjab + Himachal Pradesh + Jammu and Kashmir',
        'jk': 'Haryana + Punjab + Himachal Pradesh + Jammu and Kashmir',
        'ar': 'Arunachal Pradesh + Assam +Manipur + Meghalaya+ Mizoram + Nagaland +Sikkim +Tripura',
        'as': 'Arunachal Pradesh + Assam +Manipur + Meghalaya+ Mizoram + Nagaland +Sikkim +Tripura',
        'mn': 'Arunachal Pradesh + Assam +Manipur + Meghalaya+ Mizoram + Nagaland +Sikkim +Tripura',
        'ml': 'Arunachal Pradesh + Assam +Manipur + Meghalaya+ Mizoram + Nagaland +Sikkim +Tripura',
        'mz': 'Arunachal Pradesh + Assam +Manipur + Meghalaya+ Mizoram + Nagaland +Sikkim +Tripura',
        'nl': 'Arunachal Pradesh + Assam +Manipur + Meghalaya+ Mizoram + Nagaland +Sikkim +Tripura',
        'sk': 'Arunachal Pradesh + Assam +Manipur + Meghalaya+ Mizoram + Nagaland +Sikkim +Tripura',
        'tr': 'Arunachal Pradesh + Assam +Manipur + Meghalaya+ Mizoram + Nagaland +Sikkim +Tripura',
        'rj': 'Rajasthan',
        'mp': 'Madhya Pradesh',
        'other': 'other',
    },
}

def merge(df):
    df2 = df.copy()
    for col, mp in translation.items(): # except state
        df2[col] = df[col].map(lambda x: mp.get(x, mp['other']))
    df2 = df2.dropna() # remove invalid rows
    return df2

df2 = merge(df)

possible_geo = set([('mumbai','Maharashtra'),
('delhi','Uttar Pradesh + Uttarakhand'),
('delhi','Haryana + Punjab + Himachal Pradesh + Jammu and Kashmir'),
('delhi','other'), # last one would be in dict
('bangalore','Karnataka'),
('hyderabad','Andhra Pradesh + Telangana'),
('calcutta','West Bengal'),
('kolkata','West Bengal'),
('chennai','Tamil Nadu + Pondicherry'),
('ahmedabad','Gujarat + Goa'),
('surat','Gujarat + Goa'),
('pune','Maharashtra'),
('jaipur','Rajasthan'),
('lucknow','Uttar Pradesh + Uttarakhand'),
('kanpur','Uttar Pradesh + Uttarakhand'),
('nagpur','Maharashtra'),
('indore','Madhya Pradesh'),
('bhopal','Madhya Pradesh'),
('visakhapatnam','Gujarat + Goa'),
('patna','Bihar + Jharkhand'),
('vadodara','Gujarat + Goa'),
('ludhiana','Haryana + Punjab + Himachal Pradesh + Jammu and Kashmir'),
('agra','Uttar Pradesh + Uttarakhand'),
('nashik','Maharashtra'),
('varanasi','Uttar Pradesh + Uttarakhand'),
('dhanbad','Bihar + Jharkhand'),
('amritsar','Haryana + Punjab + Himachal Pradesh + Jammu and Kashmir'),
('allahabad','Uttar Pradesh + Uttarakhand'),
('ranchi','Bihar + Jharkhand'),
('gwalior','Madhya Pradesh'),
('coimbatore','Tamil Nadu + Pondicherry'),
('vijayawada','Andhra Pradesh + Telangana'),
('jodhpur','Rajasthan'),
('madurai','Tamil Nadu + Pondicherry'),
('raipur','Chattisgarh'),
('kochi','Kerala'),
('chandigarh','Haryana + Punjab + Himachal Pradesh + Jammu and Kashmir'),
('guwahati','Arunachal Pradesh + Assam +Manipur + Meghalaya+ Mizoram + Nagaland +Sikkim +Tripura'),
('other','Uttar Pradesh + Uttarakhand'),
('other','Maharashtra'),
('other','Bihar + Jharkhand'),
('other','West Bengal'),
('other','Chattisgarh'),
('other','Tamil Nadu + Pondicherry'),
('other','Karnataka'),
('other','Gujarat + Goa'),
('other','Andhra Pradesh + Telangana'),
('other','Odisha'),
('other','Kerala'),
('other','Haryana + Punjab + Himachal Pradesh + Jammu and Kashmir'),
('other','Arunachal Pradesh + Assam +Manipur + Meghalaya+ Mizoram + Nagaland +Sikkim +Tripura'),
('other','Rajasthan'),
('other','Madhya Pradesh'),
('other','other'),])
most_possible = dict(possible_geo)

def fix_state(row):
    city, state = row
    if (city, state) in possible_geo:
        return state
    return most_possible.get(city, 'other')

df2['old_state'] = df2.state.copy()
df2.state = df2[['city', 'old_state']].apply(fix_state, axis=1)

df3 = df2[(df2.cd.map(str) > '2022-01-01')&df2.is_cricket].groupby(basic).sum().reset_index().sort_values('reach', ascending=False)

print((df3[['reach', 'watch_time']].sort_values('reach', ascending=False).cumsum() / [df3.reach.sum(), df3.watch_time.sum()]).quantile(
    np.concatenate((np.arange(0, 0.2, 0.05), np.arange(0.2, 1, 0.1)))
).reset_index().style.format({'index':'{:.0%}', 'reach': '{:.3%}', 'watch_time': '{:.3%}'}).hide_index())

df4 = pd.concat([
    df3.reach.cumsum().rename('accum%') / df3.reach.sum(),
    df3.drop(['watch_time', 'reach',], axis=1),
    (df3.reach / df3.reach.sum()).rename('reach%'),
    (df3.watch_time / df3.watch_time.sum()).rename('watch_time%'),
], axis=1)
print(df4.head(10).style.format({'reach%': '{:.2%}', 'watch_time%': '{:.2%}', 'accum%': '{:.2%}'}).hide_index())

df7 = df2.groupby(['cd'] + basic).sum().reset_index()
df7['watch_time%'] = df7.watch_time / df7.groupby('cd').watch_time.transform('sum')
df7['reach%'] = df7.reach / df7.groupby('cd').reach.transform('sum')

df8 = df4.merge(df7, on=basic, suffixes=('', '_daily'))
df9 = df8.pivot(df4.columns, 'cd', 'reach%_daily').reset_index().fillna(0)
df9.to_csv('df9.csv', index=False, float_format='{:.2%}'.format)
df9.head(11).style.format(lambda x: f'{x:.2%}' if isinstance(x, float) else x).hide_index()

# stability day by day
df=pd.read_clipboard().dropna()
topn = int(len(df)*0.2)
ans = []
for i in df.columns:
    if i.startswith('2022'):
        s = df[i].apply(lambda x: float(x.strip('%'))/100)
        order = s.apply(lambda x: -x).argsort()
        outlier = s[:topn][order[:topn] >= topn]
        ans.append([i, len(outlier), sum(outlier)])
print(pd.DataFrame(ans, columns=['cd', 'outlier_num', 'outlier_sum']))
