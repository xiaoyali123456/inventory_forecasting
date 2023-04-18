from common import *
import sys

if __name__ == '__main__':
    cd = sys.argv[1]
    store = f's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/cms_match/cd={cd}/'
    matches = spark.sql('''
    SELECT
        contentid AS content_id,
        tournamentid AS tournament_id,
        sportsseasonid AS sportsseason_id,
        sportsseasonname,
        title,
        LOWER(shortsummary) AS shortsummary,
        hidden,
        deleted,
        premium,
        monetisable,
        replay,
        highlight,
        languageid,
        freeduration,
        duration,
        DATE_FORMAT(FROM_UNIXTIME(startdate), 'yyyy-MM-dd') AS startdate,
        DATE_FORMAT(FROM_UNIXTIME(enddate), 'yyyy-MM-dd') AS enddate
    FROM in_cms.match_update_s3
    WHERE
        contenttype = 'SPORT_LIVE'
        AND LOWER(gamename) = 'cricket'
        AND COALESCE(LOWER(shortsummary) NOT RLIKE 'mock|shadow|test stream|dummy', false)
        AND duration > 0
    ''')
    matches.repartition(1).write.parquet(store)
