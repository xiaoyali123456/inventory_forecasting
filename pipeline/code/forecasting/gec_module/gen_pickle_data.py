import pickle
import pandas as pd
from collections import defaultdict
from pyroaring import BitMap
import sys

from util import *


'''
    load sample parquet file from S3 into memory object, to save into S3 pickle file
'''


def load_from_parquet(dt, url, cols, targeting_idx_list, value_idx_list):
    # init_time = time.time()
    df = pd.read_parquet(f"{url}/cd={dt}")
    # start_time = time.time()
    # print(init_time - start_time)
    inverted_index = defaultdict(dict)  # targeting -> tag -> bitmap of sample_id
    forward_index = []

    for index, row in df.iterrows():
        for idx in targeting_idx_list:
            tags = str(row[idx]).split(',')
            # print(tags)
            for key in tags:
                if key not in inverted_index[cols[idx]]:
                    inverted_index[cols[idx]][key] = BitMap()
                inverted_index[cols[idx]][key].add(index)
        # uid, breaks, slots
        forward_index.append([row[idx] for idx in value_idx_list])

    # end_time = time.time()
    # print(end_time - start_time)
    return {'inverted_index': inverted_index, 'forward_index': forward_index}


'''
    prepare targeting options of sample data for building index
'''


def get_targeting_cols(dt, url):
    cols = list(pd.read_parquet(f"{url}/cd={dt}").columns)
    print(cols)
    none_targeting_idx_dic = {}
    none_targeting_idx_list = []
    targeting_idx_list = []
    for idx in range(len(cols)):
        if cols[idx] in ['adv_id', 'break_num', 'break_slot_count']:
            none_targeting_idx_dic[cols[idx]] = idx
        else:
            targeting_idx_list.append(idx)

    for col in ['adv_id', 'break_num', 'break_slot_count']:
        none_targeting_idx_list.append(none_targeting_idx_dic[col])
    print(none_targeting_idx_dic)
    print(none_targeting_idx_list)
    print(targeting_idx_list)
    return cols, targeting_idx_list, none_targeting_idx_list


if __name__ == '__main__':
    sample_date = get_date_list(sys.argv[1], -2)[0]
    local_pickle_path = f'sample_data_model_300'
    s3_parquet_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/gec_inventory_forecasting/vod_sampling_data_prediction_parquet_sample_rate_300/"
    s3_pickle_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/gec_inventory_forecasting/vod_sampling_bitmap_data_300/"
    cols, targeting_idx_list, value_idx_list = get_targeting_cols(sample_date, s3_parquet_path)
    os.makedirs(local_pickle_path, exist_ok=True)
    res = load_from_parquet(sample_date, s3_parquet_path, cols, targeting_idx_list, value_idx_list)
    with open(f'{local_pickle_path}/{sample_date}.pkl', 'wb') as f:
        pickle.dump(res, f)
    os.system(f"aws s3 cp {local_pickle_path}/{sample_date}.pkl {s3_pickle_path}")


