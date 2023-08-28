import sys

from util import *
from path import *

# save previous match meta data to s3
if __name__ == '__main__':
    cd = sys.argv[1]
    # cd = "2023-08-28"
    spark = hive_spark("fetch_match_cms")
    matches = spark.sql('''
    SELECT
        contentid AS content_id,
        tournamentid AS tournament_id,
        sportsseasonid AS sportsseason_id,
        sportsseasonname,
        title,
        LOWER(shortsummary) AS shortsummary,
        premium,
        monetisable,
        languageid,
        freeduration,
        DATE_FORMAT(FROM_UNIXTIME(startdate), 'yyyy-MM-dd') AS startdate,
        DATE_FORMAT(FROM_UNIXTIME(enddate), 'yyyy-MM-dd') AS enddate
    FROM in_cms.match_update_s3
    WHERE
        contenttype = 'SPORT_LIVE'
        AND LOWER(gamename) = 'cricket'
        AND COALESCE(LOWER(shortsummary) NOT RLIKE 'mock|shadow|test stream|dummy', false)
        AND COALESCE(LOWER(title) RLIKE ' vs ', false)
        AND NOT hidden
        AND NOT deleted
        AND NOT replay
        AND NOT highlight
    ORDER BY startdate DESC
    ''').distinct()
    matches.repartition(1).write.mode('overwrite').parquet(MATCH_CMS_PATH_TEMPL % cd)
