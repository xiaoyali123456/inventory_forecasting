from path import *
from util import *
from config import *


def get_break_list(playout_df, filter, tournament):
    cols = ['content_id', 'start_time', 'end_time', 'delivered_duration']
    if filter == 1:
        playout_df = playout_df \
            .withColumn('rank', F.expr('row_number() over (partition by content_id order by start_time)')) \
            .withColumn('rank_next', F.expr('rank+1'))
        res_df = playout_df \
            .join(playout_df.selectExpr('content_id', 'rank_next as rank', 'end_time as end_time_next'),
                  ['content_id', 'rank']) \
            .withColumn('bias', F.expr('cast(unix_timestamp(start_time, "yyyy-MM-dd HH:mm:ss") as long) '
                                       '- cast(unix_timestamp(end_time_next, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .where('bias >= 0') \
            .orderBy('start_time')
        res_df = playout_df \
            .where('rank = 1') \
            .select(*cols) \
            .union(res_df.select(*cols))
    elif filter == 2:
        res_df = playout_df\
            .where('content_language="hindi" and platform="android" and tenant="in"')\
            .select(*cols)
    elif filter == 3:
        res_df = playout_df\
            .where('content_language="english" and platform="android" and tenant="in"')\
            .select(*cols)
    else:
        res_df = playout_df \
            .where('content_language="english" and platform="androidtv|firetv" and tenant="in"') \
            .select(*cols)
    save_data_frame(res_df, pipeline_base_path + f"/label/break_info/start_time_data_{filter}_of_{tournament}")


def save_playout_data(spark, date, content_id, tournament):
    playout_df = load_data_frame(spark, f"{play_out_log_input_path}{date}", 'csv', True)\
        .withColumn('date', F.lit(date))\
        .withColumnRenamed(content_id_col2, 'content_id')\
        .withColumnRenamed(start_time_col2, 'start_time')\
        .withColumnRenamed(end_time_col2, 'end_time')\
        .withColumnRenamed(break_duration_col2, 'delivered_duration')\
        .withColumnRenamed(platform_col2, 'platform')\
        .withColumnRenamed(tenant_col2, 'tenant')\
        .withColumnRenamed(content_language_col2, 'content_language')\
        .withColumnRenamed(creative_id_col2, 'creative_id')\
        .withColumnRenamed(break_id_col2, 'break_id')\
        .withColumnRenamed(playout_id_col2, 'playout_id')\
        .withColumnRenamed(creative_path_col2, 'creative_path')\
        .withColumnRenamed(content_id_col, 'content_id')\
        .withColumnRenamed(start_time_col, 'start_time')\
        .withColumnRenamed(end_time_col, 'end_time')\
        .withColumnRenamed(break_duration_col, 'delivered_duration')\
        .withColumnRenamed(platform_col, 'platform')\
        .withColumnRenamed(tenant_col, 'tenant')\
        .withColumnRenamed(content_language_col, 'content_language')\
        .withColumnRenamed(creative_id_col, 'creative_id')\
        .withColumnRenamed(break_id_col, 'break_id')\
        .withColumnRenamed(playout_id_col, 'playout_id')\
        .withColumnRenamed(creative_path_col, 'creative_path')\
        .select('date', 'content_id', 'start_time', 'end_time', 'delivered_duration',
                'platform', 'tenant', 'content_language', 'creative_id', 'break_id',
                'playout_id', 'creative_path') \
        .withColumn('content_language', F.expr('lower(content_language)')) \
        .withColumn('platform', F.expr('lower(platform)')) \
        .withColumn('tenant', F.expr('lower(tenant)')) \
        .withColumn('creative_id', F.expr('upper(creative_id)')) \
        .withColumn('break_id', F.expr('upper(break_id)')) \
        .withColumn('content_id', F.trim(F.col('content_id')))\
        .where(f'content_id = "{content_id}"')
    print('unvalid stream count:')
    print(playout_df.where('content_language is null or platform is null or tenant is null').count())
    playout_df = playout_df \
        .withColumn('creative_path', F.expr('lower(creative_path)')) \
        .where('content_id != "Content ID" and start_time is not null and end_time is not null') \
        .withColumn('start_time', strip_udf('start_time')) \
        .withColumn('start_time', F.expr('if(length(start_time)==8, start_time, from_unixtime(unix_timestamp(start_time, "hh:mm:ss aa"), "HH:mm:ss"))')) \
        .withColumn('end_time', strip_udf('end_time')) \
        .withColumn('end_time', F.expr('if(length(end_time)==8, end_time, from_unixtime(unix_timestamp(end_time, "hh:mm:ss aa"), "HH:mm:ss"))')) \
        .withColumn('delivered_duration', F.expr('cast(unix_timestamp(delivered_duration, "HH:mm:ss") as long)'))
    # # .withColumn('date', F.expr('if(content_id="1540014335", "2022-05-27", date)'))\
    # # .withColumn('date', F.expr('if(content_id="1540009355", "2021-11-01", date)'))\
    # playout_df.where('date = "2023-01-05"').orderBy('start_time').show(200, False)
    if playout_df.where('start_time is null or end_time is null').count() == 0:
        playout_df = playout_df\
            .where('content_id != "Content ID" and content_id is not null '
                   'and start_time is not null and end_time is not null')\
            .withColumn('simple_start_time', F.expr('substring(start_time, 1, 5)'))\
            .withColumn('next_date', F.date_add(F.col('date'), 1))\
            .withColumn('start_date', F.expr('if(start_time < "03:00:00", next_date, date)'))\
            .withColumn('end_date', F.expr('if(end_time < "03:00:00", next_date, date)'))\
            .withColumn('start_time', F.concat_ws(" ", F.col('start_date'), F.col('start_time')))\
            .withColumn('end_time', F.concat_ws(" ", F.col('end_date'), F.col('end_time'))) \
            .withColumn('start_time_int', F.expr('cast(unix_timestamp(start_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .withColumn('end_time_int', F.expr('cast(unix_timestamp(end_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .withColumn('duration', F.expr('end_time_int-start_time_int'))\
            .where('duration > 0')
        save_data_frame(playout_df, pipeline_base_path + '/label' + playout_log_path_suffix + f"/tournament={tournament}/contentid={content_id}")
        playout_df \
            .where('creative_path != "aston"')\
            .groupBy('content_id') \
            .agg(F.min('start_time').alias('min_start_time'), F.min('end_time').alias('min_end_time'),
                 F.max('start_time').alias('max_start_time'), F.max('end_time').alias('max_end_time'),
                 F.max('duration')) \
            .withColumn('start_time_int', F.expr('cast(unix_timestamp(min_start_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .withColumn('end_time_int', F.expr('cast(unix_timestamp(max_end_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .withColumn('duration', F.expr('(end_time_int-start_time_int)/3600'))\
            .orderBy('content_id') \
            .show(1000, False)
        playout_df \
            .show(100, False)
        print(f"save {tournament} playout data done!")
    else:
        playout_df.where('start_time is null or end_time is null').groupBy('date', 'content_id').count().show()


def main(spark, date, content_id, tournament_name):
    tournament = tournament_name.replace(" ", "_").lower()
    print(tournament)
    save_playout_data(spark, date, content_id, tournament)
    playout_df = load_data_frame(spark, pipeline_base_path + playout_log_path_suffix + f"/tournament={tournament}/contentid={content_id}") \
        .where('creative_path != "aston"') \
        .cache()
    data_source = "watched_video"
    rate = 1
    wd_path = watch_video_path
    timestamp_col = "ts_occurred_ms"
    if not check_s3_path_exist(pipeline_base_path+f"/label/{data_source}/tournament={tournament}/contentid={content_id}"):
        watch_video_df = load_data_frame(spark, f"{wd_path}/cd={date}")\
            .withColumn("timestamp", F.expr(f'if(timestamp is null and {timestamp_col} is not null, '
                               f'from_unixtime({timestamp_col}/1000), timestamp)'))\
            .select("timestamp", 'received_at', 'watch_time', 'content_id', 'dw_p_id',
                    'dw_d_id')\
            .where(f'content_id = "{content_id}"')\
            .withColumn('end_timestamp', F.substring(F.col('timestamp'), 1, 19)) \
            .withColumn('end_timestamp', F.expr('if(end_timestamp <= received_at, end_timestamp, received_at)')) \
            .withColumn('watch_time', F.expr('cast(watch_time as int)')) \
            .withColumn('start_timestamp', F.from_unixtime(
                F.unix_timestamp(F.col('end_timestamp'), 'yyyy-MM-dd HH:mm:ss') - F.col('watch_time'))) \
            .withColumn('start_timestamp', F.from_utc_timestamp(F.col('start_timestamp'), "IST")) \
            .withColumn('end_timestamp', F.from_utc_timestamp(F.col('end_timestamp'), "IST")) \
            .withColumn('start_timestamp_int',
                        F.expr('cast(unix_timestamp(start_timestamp, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .withColumn('end_timestamp_int', F.expr('cast(unix_timestamp(end_timestamp, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .drop('received_at', 'timestamp', 'start_timestamp', 'end_timestamp')\
            .cache()
        save_data_frame(watch_video_df, pipeline_base_path+f"/label/{data_source}/tournament={tournament}/contentid={content_id}")
        print(f"save watch video for {tournament} done!")
    else:
        watch_video_df = load_data_frame(spark, pipeline_base_path+f"/label/{data_source}/tournament={tournament}/contentid={content_id}")\
            .cache()
        print(f"load watch video for {tournament} done!")
    filter_list = [1, 2, 3]
    for filter in filter_list:
        print(f"filter={filter}")
        get_break_list(playout_df, filter, tournament)
        final_playout_df = load_data_frame(spark, pipeline_base_path + f"/label/break_info/start_time_data_{filter}_of_{tournament}") \
            .withColumn('start_time_int', F.expr('cast(unix_timestamp(start_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .withColumn('end_time_int', F.expr('cast(unix_timestamp(end_time, "yyyy-MM-dd HH:mm:ss") as long)')) \
            .withColumn('duration', F.expr('end_time_int-start_time_int'))\
            .where('duration > 0 and duration < 3600')\
            .cache()
        # final_playout_df.where('content_id="1540009340"').orderBy('start_time_int').show(1000, False)
        print(final_playout_df.count())
        total_inventory_df = watch_video_df\
            .join(F.broadcast(final_playout_df), ['content_id'])\
            .where('(start_timestamp_int < start_time_int and end_timestamp_int > start_time_int) or (start_timestamp_int >= start_time_int and start_timestamp_int < end_time_int)')\
            .withColumn('valid_duration', F.expr('if(start_timestamp_int < start_time_int, '
                                                 'if(end_timestamp_int < end_time_int, end_timestamp_int - start_time_int, end_time_int - start_time_int), '
                                                 'if(end_timestamp_int < end_time_int, end_timestamp_int - start_timestamp_int, end_time_int - start_timestamp_int))'))\
            .withColumn('valid_duration', F.expr('cast(valid_duration as bigint)'))\
            .groupBy('date', 'content_id')\
            .agg(F.sum('valid_duration').alias('total_duration'),
                 F.countDistinct("dw_p_id").alias('total_pid_reach'),
                 F.countDistinct("dw_d_id").alias('total_did_reach'))\
            .withColumn('total_inventory', F.expr(f'cast((total_duration / 10 * {rate}) as bigint)')) \
            .withColumn('total_pid_reach', F.expr(f'cast((total_pid_reach * {rate}) as bigint)'))\
            .withColumn('total_did_reach', F.expr(f'cast((total_did_reach * {rate}) as bigint)'))\
            .cache()
        save_data_frame(total_inventory_df, pipeline_base_path + f"/label/inventory/tournament={tournament}/contentid={content_id}")
        # total_inventory_df.where('total_inventory < 0').show()
        total_inventory_df.orderBy('content_id').show()


def save_base_dataset():
    tournament_list = ['sri_lanka_tour_of_india2023', 'new_zealand_tour_of_india2023', 'wc2022', 'ac2022',
                       'south_africa_tour_of_india2022', 'west_indies_tour_of_india2022', 'ipl2022',
                       'england_tour_of_india2021', 'wc2021', 'ipl2021', 'australia_tour_of_india2020',
                       'india_tour_of_new_zealand2020', 'ipl2020', 'west_indies_tour_of_india2019', 'wc2019']
    for tournament in tournament_list:
        print(tournament)
        data_source = "watched_video_sampled"
        if not check_s3_path_exist(live_ads_inventory_forecasting_root_path + f"/final_test_dataset/{data_source}_of_{tournament}"):
            data_source = "watched_video"
        df = load_data_frame(spark, live_ads_inventory_forecasting_root_path + f"/final_test_dataset/{data_source}_of_{tournament}")
        contents = df.select('content_id').distinct().collect()
        print(contents)
        for content in contents:
            # print(content)
            content_id = content[0]
            save_data_frame(df.where(f'content_id="{content_id}"'), pipeline_base_path + f"/label/inventory/tournament={tournament}/contentid={content_id}")


strip_udf = F.udf(lambda x: x.strip(), StringType())

if __name__ == '__main__':
    # save_base_dataset()
    date, content_id, tournament_name = sys.argv[1], sys.argv[2], sys.argv[3]
    main(spark, date, content_id, tournament_name)
