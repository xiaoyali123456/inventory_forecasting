"""
 read request from booking tool api and save processed data in s3
"""
import json
import sys
import os

import pandas as pd
from datetime import datetime, timedelta
from util import s3
from path import *
import gspread


def read_google_sheet(name):
    os.system('aws s3 cp s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/minliang.lin@hotstar.com-service-account.json .')
    gc = gspread.service_account('minliang.lin@hotstar.com-service-account.json')
    x = gc.open('Inventory forecast inputs')
    df = pd.DataFrame(x.sheet1.get_all_records())
    # x = gc.open('inventory forecast input example')
    # df = pd.DataFrame(x.sheet1.get_all_records())
    df.rename(columns={'tier of team1': 'tierOfTeam1', 'tier of team2': 'tierOfTeam2'}, inplace=True)
    return df


def backfill_from_google_sheet(df):
    sheet = read_google_sheet('Inventory forecast inputs')
    match_level_features = ['matchId', 'matchDate', 'matchStartHour', 'matchType', 'matchCategory',
                            # 'matchName','fixedAdPodsPerBreak','publicHoliday',
                            'team1', 'tierOfTeam1', 'team2', 'tierOfTeam2', 'fixedBreak','averageBreakDuration','adhocBreak','adhocBreakDuration','estimatedMatchDuration','contentLanguages','platformsSupported']
    df2 = df.drop(columns=match_level_features).drop_duplicates()
    df = df2.merge(sheet, on='tournamentId')
    return df


# fromOldRequest means if the match is from old request
# matchHaveFinished means if the match is finished today
# matchShouldUpdate means if we should predict inventory and reach of the match
def load_yesterday_inputs(cd):
    yesterday = datetime.fromisoformat(cd) - timedelta(1)
    snapshot_path = PREPROCESSED_INPUT_PATH + f'cd={str(yesterday.date())}/'
    if s3.exists(snapshot_path):
        df = pd.read_parquet(snapshot_path)
    else:
        df = pd.DataFrame([], columns=['requestId', 'tournamentName', 'seasonId', 'seasonName',
       'requestStatus', 'tournamentType', 'svodFreeTimeDuration',
       'svodSubscriptionPlan', 'sportType', 'tournamentLocation',
       CUSTOM_AUDIENCE_COL, 'adPlacements', 'tournamentStartDate',
       'tournamentEndDate', 'userName', 'emailId', 'creationDate',
       'lastModifiedDate', 'error', 'tournamentId', 'matchId', 'matchCategory',
       'matchDate', 'matchStartHour', 'estimatedMatchDuration', 'matchType',
       'team1', 'tierOfTeam1', 'team2', 'tierOfTeam2', 'fixedBreak',
       'averageBreakDuration', 'adhocBreak', 'adhocBreakDuration',
       'contentLanguages', 'platformsSupported'])
    df['seasonId'] = df['seasonId'].astype(int)
    df['matchId'] = df['matchId'].astype(int)
    df['tournamentId'] = df['tournamentId'].astype(int)
    df['fromOldRequest'] = True
    df['matchHaveFinished'] = df.matchDate < cd
    any_match_finished_on_yesterday = any(df.matchDate == str(yesterday))
    df['matchShouldUpdate'] = (~df.matchHaveFinished) & any_match_finished_on_yesterday
    return df


def processing_match_stage(match_stage):
    if "group" in match_stage:
        return "group"
    elif "knockout" in match_stage:
        return "final"
    else:
        return match_stage


def unify_format(df):
    df.rename(columns={'id': 'requestId'}, inplace=True)
    team_col = 'teamResponses'
    if team_col in df.columns:
        df['team1'] = df[team_col].map(lambda x: x[0].get('name'))
        df['team2'] = df[team_col].map(lambda x: x[1].get('name'))
        df['tierOfTeam1'] = df[team_col].map(lambda x: x[0].get('tier'))
        df['tierOfTeam2'] = df[team_col].map(lambda x: x[1].get('tier'))
        df['tierOfTeam1'] = df['tierOfTeam1'].map(lambda x: x if str(x).lower().find("tier") > -1 else "tier"+str(x))
        df['tierOfTeam2'] = df['tierOfTeam2'].map(lambda x: x if str(x).lower().find("tier") > -1 else "tier"+str(x))
        df.drop(columns=team_col, inplace=True)
    match_stage_col = "matchType"
    if match_stage_col in df.columns:
        df[match_stage_col] = df[match_stage_col].map(lambda x: processing_match_stage(x))
    df['seasonId'] = df['seasonId'].astype(int)
    df['matchId'] = df['matchId'].astype(int)
    df['tournamentId'] = df['tournamentId'].astype(int)
    df['fromOldRequest'] = False
    df['matchHaveFinished'] = False  # no need to adjust for new match
    df['matchShouldUpdate'] = True
    return df


# explode match details
def convert_list_to_df(lst):
    union = []
    for req in lst:
        req = req.copy() # protect origin data
        matches = []
        for key in ['matchDetailsResponses', 'matchDetails']:
            if key in req:
                matches = req.pop(key)
        for match in matches:
            union.append({**req, **match})
    df = pd.DataFrame(union)
    df = unify_format(df)
    return df


def dump_to_json_str(x):
    if isinstance(x, str):
        return x
    else:
        try:
            x = x.tolist()
        except Exception as e:
            # Exception handling for other types of exceptions
            print("An error occurred:", str(e))
        finally:
            return json.dumps(x)


def main(cd):
    req_list = []
    page_size = 10
    i, total = 0, 1
    while i < total:
        # url = f'{BOOKING_TOOL_URL}/api/v1/inventory/forecast-request?status=INIT&page-size={page_size}&page-number={i}'
        url = f'{BOOKING_TOOL_URL}/api/v1/inventory/forecast-request?page-size={page_size}&page-number={i}'
        df = pd.read_json(url)
        req_list += df.inventoryForecastResponses.tolist()
        if len(df.totalPages) == 0:
            break
        total = df.totalPages[0]
        i += 1
        print(i)
    with s3.open(REQUESTS_PATH_TEMPL % cd, 'w') as f:
        json.dump(req_list, f)
    df_new = convert_list_to_df(req_list)
    print('df_new')
    df_new['requestDate'] = cd
    df_old = load_yesterday_inputs(cd)
    print('df_new')
    df_uni = pd.concat([df_old, df_new]).drop_duplicates(['tournamentId', 'matchId'], keep='last')
    # change list to json string because parquet doesn't support
    df_uni[['adPlacements', CUSTOM_AUDIENCE_COL, 'contentLanguages', 'platformsSupported']] = df_uni[['adPlacements', CUSTOM_AUDIENCE_COL, 'contentLanguages', 'platformsSupported']] \
        .applymap(lambda x: dump_to_json_str(x))
    print('df_new')
    # df_uni = backfill_from_google_sheet(df_uni)
    print('df_new')
    df_uni.to_parquet(PREPROCESSED_INPUT_PATH + f'cd={cd}/p0.parquet')


if __name__ == '__main__':
    cd = sys.argv[1]
    main(cd)
