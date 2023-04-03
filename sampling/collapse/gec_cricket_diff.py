import pandas as pd
wc = pd.read_clipboard()
gec = pd.read_clipboard()

top = int(len(wc) * 0.2)
basic = ['platform', 'device', 'language', 'city', 'state', 'age', 'gender']

gec2 = gec.applymap(lambda x: float(x[:-1])/100 if x.endswith('%') else x)
gec2['avg'] = (gec2['2022-11-07'] + gec2['2022-11-08'] + gec2['2022-11-11'] + gec2['2022-11-12'] + gec2['2022-11-15']) / 5
gec2.sort_values('avg', ascending=False, inplace=True)

wc2 = wc.applymap(lambda x: float(x[:-1])/100 if x.endswith('%') else x)
wc2.sort_values('reach%', ascending=False, inplace=True)

out = gec2[:top].merge(wc2[:top], on=basic, how='outer', indicator=True)
print('leaving cohort', out[out._merge == 'left_only'].avg.sum())
print('emerging cohort', out[out._merge == 'right_only']['reach%_y'].sum())

