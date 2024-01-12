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
import generate_prophet_holidays


def set_ids_as_int(df):
    id_cols = ['seasonId', 'matchId', 'tournamentId']
    for id_col in id_cols:
        if id_col in df.columns:
            df[id_col] = df[id_col].astype(int)
    return df


# fromOldRequest means if the match is from old request
# matchHaveFinished means if the match is finished today
# matchShouldUpdate means if we should predict inventory and reach of the match
def load_yesterday_request(cd):
    yesterday = datetime.fromisoformat(cd) - timedelta(1)
    snapshot_path = PREPROCESSED_INPUT_PATH + f'cd={str(yesterday.date())}/'
    if s3.exists(snapshot_path):
        df = pd.read_parquet(snapshot_path)
        df = set_ids_as_int(df)
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
    df['fromOldRequest'] = True
    df['matchHaveFinished'] = df.matchDate < cd
    any_match_finished_on_yesterday = any(df.matchDate == str(yesterday))
    print(any_match_finished_on_yesterday)
    df['matchShouldUpdate'] = (~df.matchHaveFinished) & any_match_finished_on_yesterday
    return df


def parse_match_stage(match_stage):
    if "group" in match_stage:
        return "group"
    elif "knockout" in match_stage:
        return "final"
    else:
        return match_stage


def unify_format(df, cd):
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
        df[match_stage_col] = df[match_stage_col].map(lambda x: parse_match_stage(x))
    df = set_ids_as_int(df)
    df['fromOldRequest'] = False
    if "matchDate" in df.columns:
        df['matchHaveFinished'] = df.matchDate < cd
        df['matchShouldUpdate'] = ~df.matchHaveFinished
    else:
        df['matchShouldUpdate'] = True
    return df


# explode match details
def convert_list_to_df(lst, cd):
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
    df = unify_format(df, cd)
    return df


def dump_to_json_str(x):
    print(x)
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
    page_size = 10  # why 10?
    i, total = 0, 1
    # fetch request at page level from booking tool api
    while i < total:
        # any credentials?
        url = f'{BOOKING_TOOL_URL}/api/v1/inventory/forecast-request?status=INIT&page-size={page_size}&page-number={i}'
        # url = f'{BOOKING_TOOL_URL}/api/v1/inventory/forecast-request?page-size={page_size}&page-number={i}'
        df = pd.read_json(url)
        req_list += df.inventoryForecastResponses.tolist()
        if len(df.totalPages) == 0:
            break
        total = df.totalPages[0]
        i += 1
        print(i)
    with s3.open(REQUESTS_PATH_TEMPL % cd, 'w') as f:
        json.dump(req_list, f)
    df_new = convert_list_to_df(req_list, cd)
    print('df_new')
    print(df_new)
    df_new['requestDate'] = cd
    df_old = load_yesterday_request(cd)
    print('df_old')
    print(df_old[['seasonId', 'matchId', 'tournamentId', 'matchShouldUpdate']])
    df_uni = pd.concat([df_old, df_new]).drop_duplicates(['tournamentId', 'matchId'], keep='last')
    # print(pd.concat([df_old, df_new])[['seasonId', 'matchId', 'tournamentId']])
    # change list to json string because parquet doesn't support list
    df_uni[['adPlacements', CUSTOM_AUDIENCE_COL, 'contentLanguages', 'platformsSupported']] = df_uni[['adPlacements', CUSTOM_AUDIENCE_COL, 'contentLanguages', 'platformsSupported']] \
        .applymap(lambda x: dump_to_json_str(x))
    df_uni = set_ids_as_int(df_uni)
    print('df_uni')
    # print(df_uni[['seasonId', 'matchId', 'tournamentId']])
    df_uni.to_parquet(PREPROCESSED_INPUT_PATH + f'cd={cd}/p0.parquet')
    if len(df_new) > 0:
        generate_prophet_holidays.update_features_for_prophet(df_uni, cd)


if __name__ == '__main__':
    cd = sys.argv[1]
    main(cd)
