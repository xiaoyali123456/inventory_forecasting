import pandas as pd

df = pd.read_clipboard()
df = df.rename(columns=lambda x: x.strip())
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
df['ds'] = pd.to_datetime(df.date).map(lambda x: str(x.date()))

def date_range_to_holidays(dates, holiday: str):
    return pd.DataFrame({
        'ds': dates.reset_index(drop=True).map(lambda x: str(x.date())),
        'holiday': holiday,
    })

lockdown = pd.concat([
    pd.date_range('2020-03-24', '2020-04-14').to_series(),
    pd.date_range('2020-04-15', '2020-05-03').to_series(),
    pd.date_range('2020-05-04', '2020-05-17').to_series(),
    pd.date_range('2020-05-18', '2020-05-31').to_series(),
    pd.date_range('2021-04-05', '2020-06-15').to_series(),
])
lockdown = date_range_to_holidays(lockdown, 'lockwdown')
svod = date_range_to_holidays(pd.date_range('2020-09-19', '2023-09-01').to_series(), 'svod')

def day_group(df, col):
    return df.groupby('ds')[col].max().rename('holiday').reset_index()

df2 = pd.concat([
    lockdown,
    svod,
    day_group(df, 'match_stage'),
    day_group(df[df['if_contain_india_team']==1], 'if_contain_india_team'),
    day_group(df, 'tournament_type'),
    day_group(df, 'tournament_name'),
    day_group(df, 'vod_type'),
])
df2['lower_window'] = 0
df2['upper_window'] = 0
df2.to_csv('holidays_v2.csv', index=False)
