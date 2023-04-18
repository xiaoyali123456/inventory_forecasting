from common import *

if __name__ == '__main__':
    store = 's3://'
    # tournament = spark.sql('select id as tid, name from in_cms.tournament_udpate_s3')
    # get all match, TODO: SPORT_LIVE coverage is low(16%), check live = true
    matches = spark.sql('''select contentid as content_id, shortsummary,
                        substring(from_unixtime(startdate), 1, 10) as date
                        from in_cms.watch_update_s3 
                        where contenttype="SPORT_LIVE" and lower(gamename)="cricket"
                        ''')
    # find new match
    
