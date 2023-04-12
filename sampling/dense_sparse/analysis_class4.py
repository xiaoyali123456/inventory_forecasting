#%store -r df3
import pandas as pd

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

def confusion(df, truth='class4_truth', forecast='class4_forecast'):
    gr = df.groupby(['cd', truth, forecast]).agg(
        reach=('reach_truth', 'sum'),
        num=('reach_truth', 'size')
    ).reset_index()
    gr['reach%'] = gr.reach / gr.groupby('cd').reach.transform(sum)
    gr['num%'] = gr.num / gr.groupby('cd').num.transform(sum)
    classes = ['super_dense', 'dense', 'sparse']
    metrics = ['reach%', 'num%', 'reach', 'num']
    piv = gr.pivot_table(
        index=['cd', truth],
        columns=[forecast],
        values=metrics
    ).fillna(0)
    piv2 = piv.reindex(classes, level=truth).reindex(columns=metrics, level=0).reindex(columns=classes, level=1)
    return piv2

basic = ['platform', 'language', 'city', 'state', 'age', 'device', 'gender']
ext = basic + ['custom']

df3['custom'] = df3['custom'].map(clean)
df3.cd = df3.cd.map(str)

thd = {
    'class3': [0.02, 0.3],
    'class4': [0.02, 0.2],
}

df5 = df3.groupby(['cd'] + ext).reach.sum().reset_index()
df5['rank'] = df5.groupby(['cd']).reach.rank(method='first', ascending=False, pct=True)
for k, v in thd.items():
    df5[k] = df5['rank'].map(lambda x:classisfy(x, *v))

sep = '2022-11-01'
df3b = df3[(df3.cd < sep)].groupby(['cd'] + basic).reach.sum().reset_index() # XXX: deduplication
history = df3b.groupby(basic).mean().reset_index()
history['rank'] = history.reach.rank(method='first', ascending=False, pct=True)
for k, v in thd.items():
    history[k] = history['rank'].map(lambda x:classisfy(x, *v))

df7 = df5[df5.cd >= sep].merge(history, on=basic, how='left', suffixes=['_truth', '_forecast'])

# class 3
t = confusion(df7, 'class3_truth', 'class3_forecast')
print(t.to_csv())
print(t.mean(level=1).to_csv())

# class 4
t = confusion(df7, 'class4_truth', 'class4_forecast')
print(t.to_csv())
print(t.mean(level=1).to_csv())
