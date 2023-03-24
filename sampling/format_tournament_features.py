import pandas as pd

df = pd.read_clipboard()
df = df.rename(columns=lambda x:x.strip())
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
df['ds'] = pd.to_datetime(df.date).map(lambda x:str(x.date()))

covid_lockdown = pd.concat([
    pd.date_range('2020-03-24', '2020-04-14').to_series(),
    pd.date_range('2020-04-15', '2020-05-03').to_series(),
    pd.date_range('2020-05-04', '2020-05-17').to_series(),
    pd.date_range('2020-05-18', '2020-05-31').to_series(),
    pd.date_range('2021-04-05', '2020-06-15').to_series(),
])

covid_lockdown.map(lambda x: str(x.date()))
svod_dates = pd.date_range('2020-09-19', '2023-09-01').to_series()

df[['ds', 'match_stage', 'if_contain_india_team', 'tournament_type']] # 'vod_type'
df2 = pd.concat([
    df[['ds', 'match_stage']].rename(columns={'match_stage':'holiday'}),
    df[['ds', 'if_contain_india_team']].rename(columns={'if_contain_india_team':'holiday'}),
    df[['ds', 'tournament_type']].rename(columns={'tournament_type':'holiday'}),
])

