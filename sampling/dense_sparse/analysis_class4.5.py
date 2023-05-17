# use WC2022 to forecast future tournament
import pandas as pd

replace = {
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
    'gender': {
        'm': 'male',
        'other': 'female',
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

most_possible = dict([
    ('mumbai','Maharashtra'),
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
    ('other','other'),
])

def fix_state(row):
    city, state = row
    if (city, state) in most_possible:
        return state
    return most_possible.get(city, 'other')

def clean(tag):
    lst = [t for t in tag.split('|') if '_NA' not in t]
    res = '|'.join(lst)
    return 'other' if res == '' else res

def merge(df):
    df2 = df.copy()
    for col, mp in replace.items():
        df2[col] = df[col].map(lambda x: mp.get(x, mp['other']))
    df2 = df2.dropna() # remove invalid rows
    df2['state'] = df2[['city', 'state']].apply(fix_state, axis=1)
    return df2

def classisfy(rank, thd1=0.05, thd2=0.2):
    if rank <= thd1:
        return 'super_dense'
    elif rank <= thd1 + thd2:
        return 'dense'
    else:
        return 'sparse'

def rank(df, group):
    df = df.loc[df.is_cricket&~df.is_cricket.isna()]
    df.reach *= 4
    df.watch_time *= 4
    df2 = merge(df)
    df2['custom'] = df2['custom'].map(clean)
    df2.cd = df2.cd.map(str)
    df3 = df2.groupby(group).reach.sum().reset_index()
    # TODO: we fix `cd` here
    df3['rank'] = df3.groupby('cd').reach.rank(method='first', ascending=False, pct=True)
    df3['class'] = df3['rank'].map(lambda x:classisfy(x, 0.02, 0.2))


h = pd.read_parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/dense_sparse/qdata_v3/')

df2 = pd.read_parquet('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/dense_sparse/v4/')
df2 = df2[df2.is_cricket&~df2.is_cricket.isna()]
df2.reach *= 4
df2.watch_time *= 4

df3 = merge(df2)
df3['custom'] = df3['custom'].map(clean)
df3.cd = df3.cd.map(str)

basic = ['platform', 'language', 'city', 'state', 'age', 'device', 'gender']
df5 = df3.groupby(['cd'] + basic).reach.sum().reset_index()
df5['rank'] = df5.groupby(['cd']).reach.rank(method='first', ascending=False, pct=True)
df5['class'] = df5['rank'].map(lambda x:classisfy(x, 0.02, 0.3))

