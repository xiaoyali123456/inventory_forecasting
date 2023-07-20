"""
 read request from booking tool api and save processed data in s3
"""
import json
import sys
import os

import pandas as pd
from datetime import datetime, timedelta
from common import REQUESTS_PATH_TEMPL, BOOKING_TOOL_URL, PREPROCESSED_INPUT_PATH, s3
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
    match_level_features = ['matchId','matchDate','matchStartHour','matchType','matchCategory',
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
       'customAudiences', 'adPlacements', 'tournamentStartDate',
       'tournamentEndDate', 'userName', 'emailId', 'creationDate',
       'lastModifiedDate', 'error', 'tournamentId', 'matchId', 'matchCategory',
       'matchDate', 'matchStartHour', 'estimatedMatchDuration', 'matchType',
       'team1', 'tierOfTeam1', 'team2', 'tierOfTeam2', 'fixedBreak',
       'averageBreakDuration', 'adhocBreak', 'adhocBreakDuration',
       'contentLanguages', 'platformsSupported'])
    df['fromOldRequest'] = True
    df['matchHaveFinished'] = df.matchDate < cd
    any_match_finished_on_yesterday = any(df.matchDate == str(yesterday))
    df['matchShouldUpdate'] = (cd <= df.tournamentEndDate) & (~df.matchHaveFinished) & any_match_finished_on_yesterday
    df['matchShouldUpdate'] = False  # TODO
    return df


def unify_format(df):
    df.rename(columns={'id': 'requestId'}, inplace=True)
    if 'team' in df.columns:
        df['team1'] = df['team'].map(lambda x:x[0].get('name'))
        df['team2'] = df['team'].map(lambda x:x[1].get('name'))
        df['tierOfTeam1'] = df['team'].map(lambda x:x[0].get('tier'))
        df['tierOfTeam2'] = df['team'].map(lambda x:x[1].get('tier'))
        df['tierOfTeam1'] = df['tierOfTeam1'].map(lambda x:x if str(x).find("tier") > -1 else "tier"+str(x))
        df['tierOfTeam2'] = df['tierOfTeam2'].map(lambda x:x if str(x).find("tier") > -1 else "tier"+str(x))
        df.drop(columns='team', inplace=True)
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


def main(cd):
    req_list = []
    page_size = 10
    i, total = 0, 1
    while i < total:
        url = f'{BOOKING_TOOL_URL}/api/v1/inventory/forecast-request?page-size={page_size}&page-number={i}'
        df = pd.read_json(url)
        req_list += df.inventoryForecastResponses.tolist()
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
    df_uni[['adPlacements', 'customAudiences', 'contentLanguages', 'platformsSupported']] = df_uni[['adPlacements', 'customAudiences', 'contentLanguages', 'platformsSupported']] \
        .applymap(lambda x: x if isinstance(x, str) else json.dumps(x))
    print('df_new')
    # df_uni = backfill_from_google_sheet(df_uni)
    print('df_new')
    df_uni.to_parquet(PREPROCESSED_INPUT_PATH + f'cd={cd}/p0.parquet')


if __name__ == '__main__':
    cd = sys.argv[1]
    main(cd)
