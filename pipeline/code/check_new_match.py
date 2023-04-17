from common import *

if __name__ == '__main__':
    # tournament = spark.sql('select id as tid, name from in_cms.tournament_udpate_s3')
    matches = spark.sql('''select tournamentid as tid, contentid as content_id, shortsummary,
                        substring(from_unixtime(startdate), 1, 10) as date,
                        from in_cms.watch_update_s3 where contenttype="SPORT_LIVE" and lower(gamename)="cricket"
                        ''')
