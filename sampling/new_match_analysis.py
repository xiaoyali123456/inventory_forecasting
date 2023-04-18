import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder \
        .config("spark.sql.hive.convertMetastoreParquet", "false") \
        .config("hive.metastore.uris", "thrift://metastore.data.hotstar-labs.com:9083") \
        .enableHiveSupport() \
        .getOrCreate()
sc.setLogLevel("ERROR")

df = spark.sql('select live, contenttype from in_cms.match_update_s3')
pdf = df.groupby(df.columns).count().sort(df.columns).toPandas()
print(pdf.to_string(index=False))
'''
 live            contenttype  count
False                  CLIPS      2
False             NEWS_CLIPS     13
False                  SPORT  76735
False      SPORTS_VIDEO_FEED  10840
False            SPORT_CLIPS      2
False       SPORT_HIGHLIGHTS     17
False SPORT_MATCH_HIGHLIGHTS  18800
False      SPORT_MATCH_RECAP      5
False           SPORT_REPLAY  13647
 True              SHOW_LIVE      6
 True             SPORT_LIVE  22102
'''

df2 = spark.sql('''select lower(primarygenre) as g1,
    contentid,
    lower(gamename) as g2,
    shortsummary,
    s_title,
    genre,
    status,
    tournamentid,
    contenttype,
    substring(from_unixtime(startdate), 1, 10) as startdate
    from in_cms.match_update_s3
    where lower(primarygenre)='cricket' or lower(gamename)='cricket'
''')
# pdf2 = df2.groupby('g1', 'g2').count().sort('g1', 'g2').toPandas()
# print(pdf2.to_string(index=False))

# '''
#        g1                 g2  count                                             
#      None            cricket  47098
# badminton            cricket      4
#   cricket               None      5
#   cricket          athletics      1
#   cricket            cricket  11870
#   cricket           football    297
#   cricket             hockey      1
#   cricket            kabaddi      3
#   cricket mixed martial arts      1
#   cricket              other      2
#    hockey            cricket      8
#   kabaddi            cricket      1
#      news            cricket     13
#    tennis            cricket      1
# '''

df2.where('g1 != g2')[['contentid', 'tournamentid', 'shortsummary', 'status']].show(10, False)
df2.where('g1 is null')[['contentid', 'tournamentid', 'shortsummary', 'status']].show(10, False)
df2.where('g1 == g2')[['contentid', 'tournamentid', 'shortsummary', 'status']].show(10, False)

# Step 2
df3 = df2.where('g1 != g2').toPandas()
def f(x):
    if x is None:
        return False
    x = x.lower()
    for y in ['mock', 'shadow', 'test stream', 'dummy']:
        if y in x:
            return False
    return True
df3[df3.shortsummary.map(f)]
f2 = F.udf(f, returnType=BooleanType())
print(df2[f2('shortsummary')].where('contenttype="SPORT_LIVE"').count()) # 6551
df2[f2('shortsummary')].where('contenttype="SPORT_LIVE" and g2 is null').count() # 0!

# Step 3
df2[f2('shortsummary')].where('contenttype="SPORT_LIVE" and g2 != "cricket"').show()
print(df2[f2('shortsummary')].where('contenttype="SPORT_LIVE" and g2 != "cricket"').count()) #64
df2[f2('shortsummary')].where('contenttype="SPORT_LIVE" and g2 != "cricket"')[['shortsummary']].show(10, False)

# Step 4
matches = spark.sql('''
    SELECT
        contentid AS content_id,
        tournamentid AS tournament_id,
        sportsseasonid AS sportsseason_id,
        sportsseasonname,
        title,
        LOWER(shortsummary) AS shortsummary,
        CAST(hidden AS INT) AS hidden,
        CAST(deleted AS INT) AS deleted,
        CAST(premium AS INT) AS premium,
        CAST(vip AS INT) AS vip,
        CAST(monetisable AS INT) AS monetisable,
        CAST(replay AS INT) AS replay,
        CAST(highlight AS INT) AS highlight,
        languageid,
        freeduration, duration,
        DATE_FORMAT(FROM_UNIXTIME(startdate), 'yyyy-MM-dd') AS startdate,
        DATE_FORMAT(FROM_UNIXTIME(enddate), 'yyyy-MM-dd') AS enddate
    FROM in_cms.match_update_s3
    WHERE
        contenttype = 'SPORT_LIVE'
        AND LOWER(gamename) = 'cricket'
        AND COALESCE(LOWER(shortsummary) NOT RLIKE 'mock|shadow|test stream|dummy', false)
''')
matches.describe("hidden deleted premium vip monetisable replay highlight languageid freeduration duration".split())
'''
+-------+--------------------+-------+-------------------+-------------------+-------------------+------+---------+----------+-----------------+------------------+
|summary|              hidden|deleted|            premium|                vip|        monetisable|replay|highlight|languageid|     freeduration|          duration|
+-------+--------------------+-------+-------------------+-------------------+-------------------+------+---------+----------+-----------------+------------------+
|  count|                6357|   6357|               6357|               6357|               6357|  6357|     6357|      6357|             6258|              6357|
|   mean|0.029416391379581564|    0.0|0.20418436369356616|0.20418436369356616| 0.9134812018247601|   0.0|      0.0|      null|256.2607861936721| 77.43102092181847|
| stddev| 0.16898390244179615|    0.0|0.40313604977284717|0.40313604977284717|0.28115072501557026|   0.0|      0.0|      null|137.5172573458075|1785.6292574408449|
|    min|                   0|      0|                  0|                  0|                  0|     0|        0|      [10]|                0|                 0|
|    max|                   1|      0|                  1|                  1|                  1|     0|        0|       [9]|             3300|             42000|
+-------+--------------------+-------+-------------------+-------------------+-------------------+------+---------+----------+-----------------+------------------+
'''

# hidden => duration == 0
matches[matches.hidden>0].describe("hidden deleted premium vip monetisable replay highlight languageid freeduration duration".split()).show()
