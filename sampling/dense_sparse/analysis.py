import pandas as pd

# input_root = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/dense_sparse/qdata_v2/'
# df = spark.read.parquet(input_root)
# df2 = df[df.is_cricket].toPandas()

# prefer local loading due to save memory
df2 = pd.read_parquet('qdata_v2')
df2 = df2[df2.is_cricket&~df2.is_cricket.isna()]

df2.reach *= 4
df2.watch_time *= 4

# rdf = pd.read_csv('s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/dense_sparse/reach/wc2022.csv')
# rdf.reach *= 4
# df3 = df2.groupby('cd').reach.sum().reset_index()
# (df3.reach / rdf.reach).describe()

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
    'custom': {
        '': 'other',
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

possible_geo = set([
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
most_possible = dict(possible_geo)

def fix_state(row):
    city, state = row
    if (city, state) in possible_geo:
        return state
    return most_possible.get(city, 'other')

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

def clean(tag):
    lst = [t for t in tag.split('|') if '_NA' not in t]
    res = '|'.join(lst)
    return 'other' if res == '' else res

df3 = merge(df2)
df3['custom'] = df3['custom'].map(clean)
df3.cd = df3.cd.map(str)

# look num of custom cohort
basic = ['platform', 'language', 'city', 'state', 'age', 'device', 'gender']
ext = basic + ['custom']
x = df3[['cd'] + basic].drop_duplicates().groupby('cd').size()
y = df3[['cd'] + ext].drop_duplicates().groupby('cd').size()
(y / x).describe()

df4 = df3.groupby(['cd'] + basic).reach.sum().reset_index()
df4['rank'] = df4.groupby(['cd']).reach.rank(method='first', ascending=False, pct=True)
df4['total'] = df4.groupby(['cd']).reach.transform('size')
df4['class3'] = df4['rank'].map(lambda x:classisfy(x, 0.02, 0.2))
# df4['class1'] = df4['rank'].map(classisfy)
# df4['class2'] = df4['rank'].map(lambda x:classisfy(x, 0.05, 0.3))

df5 = df3.groupby(['cd'] + ext).reach.sum().reset_index()
df5['rank'] = df5.groupby(['cd']).reach.rank(method='first', ascending=False, pct=True)
df5['total'] = df5.groupby(['cd']).reach.transform('size')
df5['class3'] = df5['rank'].map(lambda x:classisfy(x, 0.02, 0.2))
# df5['class1'] = df5['rank'].map(classisfy)
# df5['class2'] = df5['rank'].map(lambda x:classisfy(x, 0.3))

df6 = df5.merge(df4, on=['cd']+basic, how='left', suffixes=['_truth', '_forecast'])
def confusion(df, truth='class3_truth', forecast='class3_forecast'):
    gr = df.groupby(['cd', truth, forecast]).agg(
        reach=('reach_truth', 'sum'),
        num=('reach_truth', 'size')
    ).reset_index()
    gr['reach%'] = gr.reach / gr.groupby('cd').reach.transform(sum)
    gr['num%'] = gr.num / gr.groupby(['cd', truth]).num.transform(sum)
    piv = gr.pivot_table(
        index='cd',
        columns=[truth, forecast],
        values=['reach', 'reach%', 'num', 'num%']
    ).fillna(0).reset_index()
    return piv

# Analyze the cause of misclassification
# %store -r df6
# confusion(df6, 'class1_x', 'class1_y')
# df6[(df6.cd.map(str) == '2022-10-23')&(df6.class1_x == 'super_dense')&(df6.class1_y == 'dense')]
# df6[(df6.class1_x == 'super_dense')&(df6.class1_y == 'sparse')]

# Improve based on threshold
# confusion(df6, 'class2_x', 'class2_y')
# df6[(df6.class2_x == 'super_dense')&(df6.class2_y == 'dense')].to_csv()

# Reduce the super-dense to adapt the real capacity
t = confusion(df6, 'class3_truth', 'class3_forecast')
print(t['reach%'])
print(t.to_csv()) # for copy to google sheet
df6[(df6.cd.map(str) == '2022-10-23')&(df6.class3_truth == 'super_dense')&(df6.class3_forecast == 'dense')].to_csv()

# time series forecast
sep = '2022-11-01'
history = df3[df3.cd < sep].groupby(basic).agg({'reach': 'mean'}).reset_index()
history['rank'] = history.reach.rank(method='first', ascending=False, pct=True)
history['class3'] = history['rank'].map(lambda x:classisfy(x, 0.02, 0.2))

df7 = df5[df5.cd >= sep].merge(history, on=basic, how='left', suffixes=['_truth', '_forecast'])
#%store -r df7
t = confusion(df7, 'class3_truth', 'class3_forecast')
print(t['reach%'])
print(t.to_csv())
