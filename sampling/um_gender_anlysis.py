# read UM and 
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

spark = SparkSession.builder \
        .config("spark.sql.hive.convertMetastoreParquet", "false") \
        .config("hive.metastore.uris", "thrift://metastore.data.hotstar-labs.com:9083") \
        .enableHiveSupport() \
        .getOrCreate()

@F.udf(returnType=StringType())
def parse(segments):
    s = ''
    f = [
        'FB_FEMALE_13-17',
        'FB_FEMALE_18-24',
        'FB_FEMALE_25-34',
        'FB_FEMALE_TV_2-14',
        'FB_FEMALE_TV_15-21',
        'FB_FEMALE_TV_22-30',
        'FB_BARC_FEMALE_15-21',
        'FB_BARC_FEMALE_22-30',
        'EMAIL_FEMALE_13-17',
        'EMAIL_FEMALE_18-24',
        'EMAIL_FEMALE_25-34',
        'EMAIL_BARC_FEMALE_15-21',
        'EMAIL_BARC_FEMALE_22-30',
        'PHONE_FEMALE_13-17',
        'PHONE_FEMALE_18-24',
        'PHONE_FEMALE_25-34',
        'PHONE_FEMALE_TV_2-14',
        'PHONE_FEMALE_TV_15-21',
        'PHONE_FEMALE_TV_22-30',
        'PHONE_BARC_FEMALE_15-21',
        'PHONE_BARC_FEMALE_22-30',
        'FMD009V0051317SRMLDESTADS',
        'FMD009V0051317HIGHSRMLDESTADS',
        'FMD009V0051334HIGHSRMLDESTADS',
        'FMD009V0051334SRMLDESTADS',
        'FMD009V0051824HIGHSRMLDESTADS',
        'FMD009V0051824SRMLDESTADS',
        'FMD009V0052534HIGHSRMLDESTADS',
        'FMD009V0052534SRMLDESTADS',
    ]
    m = [
        'FB_MALE_13-17',
        'FB_MALE_18-24',
        'FB_MALE_25-34',
        'FB_MALE_TV_2-14',
        'FB_MALE_TV_15-21',
        'FB_MALE_TV_22-30',
        'FB_BARC_MALE_15-21',
        'FB_BARC_MALE_22-30',
        'FB_MALE_13-17',
        'FB_MALE_18-24',
        'FB_MALE_25-34',
        'FB_MALE_TV_2-14',
        'FB_MALE_TV_15-21',
        'FB_MALE_TV_22-30',
        'FB_BARC_MALE_15-21',
        'FB_BARC_MALE_22-30',
        'PHONE_MALE_13-17',
        'PHONE_MALE_18-24',
        'PHONE_MALE_25-34',
        'PHONE_BARC_MALE_15-21',
        'PHONE_BARC_MALE_22-30',
        'PHONE_MALE_TV_2-14',
        'PHONE_MALE_TV_15-21',
        'PHONE_MALE_TV_22-30',
        'MMD009V0051317HIGHSRMLDESTADS',
        'MMD009V0051317SRMLDESTADS',
        'MMD009V0051334HIGHSRMLDESTADS',
        'MMD009V0051334SRMLDESTADS',
        'MMD009V0051824HIGHSRMLDESTADS',
        'MMD009V0051824SRMLDESTADS',
        'MMD009V0052534HIGHSRMLDESTADS',
        'MMD009V0052534SRMLDESTADS',
    ]
    dc = {'f': f, 'm': m}
    if isinstance(segments, str):
        for x in dc:
            for y in dc[x]:
                if y in segments:
                    s += x
                    break
    return s


um = spark.sql('select sha2(pid, 256) as dw_p_id, gender as gender_um from in_ums.user_umfnd_s3')
df = spark.sql('select dw_p_id, user_segments from data_lake.watched_video where cd == "2023-03-01"')
df2 = df.select('dw_p_id', parse('user_segments').alias('gender_wv'))
df3 = um.sample(0.1, 0).join(df2.sample(0.1, 0), on='dw_p_id')
df4 = df3.groupby('gender_um', 'gender_wv').count().toPandas()

