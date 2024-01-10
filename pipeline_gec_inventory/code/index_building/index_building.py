import pickle
import pandas as pd
from collections import defaultdict
from pyroaring import BitMap
import sys

from gec_util import *
from gec_path import *


'''
    load sample parquet file from S3 into memory object, convert it to bitmap data and save as the S3 pickle file
'''


# inventory 12mn, 1.8 sample --> 4.8 sample, inventory 24mn; 10 inv * 300 * 2
# {key1: {key2: value}}, key1: gender, key2: male, value: set of sample_id
# col = ["gender", "age", ...]
def index_building(dt, url, cols, targeting_idx_list, value_idx_list):
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
        forward_index.append([int(row[idx]) for idx in value_idx_list])
    # end_time = time.time()
    # print(end_time - start_time)
    return {'inverted_index': inverted_index, 'forward_index': forward_index}


'''
    prepare targeting options of sample data for building index
'''


def get_targeting_cols(dt, url):
    cols = list(pd.read_parquet(f"{url}/cd={dt}").columns)
    # print(cols)
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
    # print(none_targeting_idx_dic)
    # print(none_targeting_idx_list)
    # print(targeting_idx_list)
    return cols, targeting_idx_list, none_targeting_idx_list


if __name__ == '__main__':
    sample_date = get_yesterday(sys.argv[1])
    print(sample_date)
    local_pickle_path = f'sample_data_model_300'
    cols, targeting_idx_list, value_idx_list = get_targeting_cols(sample_date, VOD_SAMPLING_DATA_PREDICTION_PARQUET_PATH)
    os.makedirs(local_pickle_path, exist_ok=True)
    res = index_building(sample_date, VOD_SAMPLING_DATA_PREDICTION_PARQUET_PATH, cols, targeting_idx_list, value_idx_list)
    with open(f'{local_pickle_path}/{sample_date}.pkl', 'wb') as f:
        pickle.dump(res, f)
    os.system(f"aws s3 cp {local_pickle_path}/{sample_date}.pkl {VOD_BITMAP_PICKLE_PATH}")


