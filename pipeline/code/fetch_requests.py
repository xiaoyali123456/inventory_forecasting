import json
import sys

import pandas as pd
from datetime import datetime, timedelta
from common import REQUESTS_PATH_TEMPL, BOOKING_TOOL_URL, PREPROCESSED_INPUT_PATH, s3

def backfill_from_google_sheet():
    pass

def load_yesterday_inputs(cd):
    yesterday = datetime.fromisoformat(cd) - timedelta(1)
    snapshot_path = PREPROCESSED_INPUT_PATH + f'cd={str(yesterday.date())}/'
    if s3.exists(snapshot_path):
        df = pd.read_parquet(snapshot_path)
    else:
        df = pd.DataFrame([], columns=['requestId', 'tournamentName', 'seasonId', 'seasonName',
       'requestStatus', 'tournamentType', 'svodFreeTimeDuration',
       'svodSubscriptionPlan', 'sportType', 'tournamentLocation',
       'customAudiences', 'adPlacements', 'tournamentStartDate',
       'tournamentEndDate', 'userName', 'emailId', 'creationDate',
       'lastModifiedDate', 'error', 'tournamentId', 'matchId', 'matchCategory',
       'matchDate', 'matchStartHour', 'estimatedMatchDuration', 'matchType',
       'team1', 'tierOfTeam1', 'team2', 'tierOfTeam2', 'fixedBreak',
       'averageBreakDuration', 'adhocBreak', 'adhocBreakDuration',
       'contentLanguages', 'platformsSupported'])
    df['fromOldRequest'] = False
    df['matchHaveFinished'] = df.matchDate < cd
    finish_on_yesterday = any(df.matchDate == str(yesterday))
    df['matchShouldUpdate'] = (cd <= df.tournamentEndDate) & (~df.matchHaveFinished) & finish_on_yesterday
    return df

def unify_format(df):
    df.rename(columns={'id': 'requestId'}, inplace=True)
    if 'team' in df.columns:
        df['team1'] = df['team'].map(lambda x:x[0].get('name'))
        df['team2'] = df['team'].map(lambda x:x[1].get('name'))
        df['tierOfTeam1'] = df['team'].map(lambda x:x[0].get('tier'))
        df['tierOfTeam2'] = df['team'].map(lambda x:x[1].get('tier'))
        df.drop(columns='team', inplace=True)
    df['fromOldRequest'] = False
    df['matchHaveFinished'] = False # no need to adjust for new match
    df['matchShouldUpdate'] = True
    return df

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

def main(cd):
    req_list = []
    page_size = 10
    i, total = 1, 1
    while i <= total:
        url = (f'{BOOKING_TOOL_URL}inventory/forecast-request?status=INIT'
               f'&page-number={i}'
               f'&page-size={page_size}')
        df = pd.read_json(url)
        req_list += df.results.tolist()
        total = df.total_pages[0]
        i += 1
    with s3.open(REQUESTS_PATH_TEMPL % cd, 'w') as f:
        json.dump(req_list, f)
    df_new = convert_list_to_df(req_list)
    df_new['requestDate'] = cd
    df_old = load_yesterday_inputs(cd)
    df_uni = pd.concat([df_old, df_new]).drop_duplicates(['tournamentId', 'matchId'], keep='last')
    df_uni[['adPlacements', 'customAudiences', 'contentLanguages', 'platformsSupported']] = df_uni[['adPlacements', 'customAudiences', 'contentLanguages', 'platformsSupported']].applymap(json.dumps)
    df_uni.to_parquet(PREPROCESSED_INPUT_PATH + f'cd={cd}/p0.parquet')


if __name__ == '__main__':
    cd = sys.argv[1]
    main(cd)
