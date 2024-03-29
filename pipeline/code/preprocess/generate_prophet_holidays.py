import pandas as pd
import os

from path import *
from util import get_last_cd


def update_features_for_prophet(df: pd.DataFrame, cd):
    df = df[df['fromOldRequest'] == False]
    df['abandoned'] = 0
    df['if_contain_india_team'] = df.apply(
        lambda row: 1 if 'india' in [row['team1'].lower(), row['team2'].lower()] else 0, axis=1)
    cols = ['date', 'abandoned', 'vod_type', 'match_stage', 'tournament_name',
            'match_type', 'if_contain_india_team', 'tournament_type']
    df = df.rename(columns={'matchDate': 'date',
                            'tournamentType': 'vod_type',
                            'matchType': 'match_stage',
                            'tournamentName': 'tournament_name',
                            'matchCategory': 'match_type'})
    df[['vod_type', 'match_stage', 'tournament_name', 'match_type']] = df[
        ['vod_type', 'match_stage', 'tournament_name', 'match_type']].apply(lambda x: x.str.lower())
    df['tournament_type'] = df.apply(
        lambda row: "national" if ("ipl" in row['tournament_name'] or "ranji trophy" in row['tournament_name']) else (
            "tour" if "tour" in row['tournament_name'] else "international"), axis=1)
    last_cd = get_last_cd(PROPHET_FEATURES_PATH, cd)
    old_df = pd.read_csv(f"{PROPHET_FEATURES_PATH}/cd={last_cd}/feature.csv").rename(columns=lambda x: x.strip())
    new_df = pd.merge(old_df['date'], df[cols], on='date', how='right')
    res_df = pd.concat([old_df[cols], new_df[cols]])
    res_df.to_csv(f"{PROPHET_FEATURES_PATH}/cd={cd}/feature.csv", index=False)
    generate_holidays(f"{PROPHET_FEATURES_PATH}/cd={cd}/feature.csv", f"{PROPHET_HOLIDAYS_PATH}/cd={cd}/holidays.csv")


def generate_holidays(input_file, output_file):
    df0 = pd.read_csv(input_file)

    df = df0.rename(columns=lambda x: x.strip())
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df['ds'] = pd.to_datetime(df.date).map(lambda x: str(x.date()))
    df = df[df.abandoned != 1].copy()

    def date_range_to_holidays(dates, holiday: str):
        return pd.DataFrame({
            'ds': dates.reset_index(drop=True).map(lambda x: str(x.date())),
            'holiday': holiday,
        })

    lockdown = pd.concat([
        pd.date_range('2020-03-24', '2020-05-31').to_series(),
        # pd.date_range('2020-03-24', '2020-04-14').to_series(),
        # pd.date_range('2020-04-15', '2020-05-03').to_series(),
        # pd.date_range('2020-05-04', '2020-05-17').to_series(),
        # pd.date_range('2020-05-18', '2020-05-31').to_series(),
        # pd.date_range('2021-04-05', '2021-06-15').to_series(),
    ])
    lockdown = date_range_to_holidays(lockdown, 'lockwdown')
    svod = date_range_to_holidays(pd.date_range('2020-03-29', '2023-08-23').to_series(), 'svod_dates')
    # svod = date_range_to_holidays(pd.date_range('2020-09-19', '2023-08-23').to_series(), 'svod_dates')

    def day_group(df, col):
        return df.groupby('ds')[col].max().rename('holiday').reset_index()

    df['if_contain_india_team'].replace({1: 'india_team', 0: 'no_india_team'}, inplace=True)
    df['super_match'] = df['if_contain_india_team'] + '_' + df['tournament_type'] + '_' + df['vod_type']

    df2 = pd.concat([
        lockdown,
        svod,
        day_group(df, 'match_stage').dropna(),
        day_group(df, 'match_type'),
        day_group(df[df['if_contain_india_team'] == 'india_team'], 'if_contain_india_team'),
        day_group(df, 'tournament_type'),
        day_group(df, 'tournament_name'),
        day_group(df, 'vod_type'),
        day_group(df[df['if_contain_india_team'] == 'india_team'], 'super_match'),
    ])

    df2['lower_window'] = 0
    df2['upper_window'] = 0
    df2.to_csv(output_file, index=False)
    print(len(df2))


if __name__ == '__main__':
    input_file = "data/prophet_features.csv"
    output_file = "data/prophet_holidays.csv"
    # print('pwd')
    # print(os.system('pwd'))
    generate_holidays(input_file, output_file)
    os.system(f'aws s3 cp {output_file} {"/".join(PROPHET_HOLIDAYS_PATH.split("/")[-1:])}')
