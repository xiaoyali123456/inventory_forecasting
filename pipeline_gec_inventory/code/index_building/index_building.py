import pickle
import pandas as pd
from collections import defaultdict
from pyroaring import BitMap
import sys
import base64

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
    inverted_index = defaultdict(dict)  # targeting col -> targeting col value -> bitmap of sample_id
    forward_index = []  # sample's non-targeting cols' value list: 'adv_id', 'break_num', 'break_slot_count'
    for index, row in df.iterrows():
        for idx in targeting_idx_list:
            tags = str(row[idx]).split(',')
            # print(tags)
            for key in tags:
                if key not in inverted_index[cols[idx]]:
                    inverted_index[cols[idx]][key] = BitMap()  # dense storage, like 00110100
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
    # Q: how does the data in VOD_SAMPLE_PARQUET_PATH come from? A: result of sampling.py
    cols = list(pd.read_parquet(f"{url}/cd={dt}").columns)
    # print(cols)
    none_targeting_idx_dic = {}  # store col index: {col: idx}
    none_targeting_idx_list = []  # store col index: [idx]
    targeting_idx_list = []  # store targeting col index: [idx]
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

def snake_to_camel(snake_str):
    components = snake_str.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])

def format_targeting_col(inverted_index):
    result = {}
    for key, value in inverted_index.items():
        new_key = snake_to_camel(key)
        result[new_key] = value
    return result

def dump_index_to_json(res):
    inverted_index = format_targeting_col(res['inverted_index'])
    for key in inverted_index:
        for tag in inverted_index[key]:
            print(key, tag)
            bm = inverted_index[key][tag]
            val = base64.b64encode(bm.serialize())
            inverted_index[key][tag] = val.decode('utf-8')
    res['inverted_index'] = inverted_index
    return json.dumps(res)

if __name__ == '__main__':
    sample_date = get_yesterday(sys.argv[1])
    print(sample_date)
    local_pickle_path = f'sample_data_model_300'

    # get the targeting col dict, list and non-targeting col list
    cols, targeting_idx_list, value_idx_list = get_targeting_cols(sample_date, VOD_SAMPLING_DATA_PREDICTION_PARQUET_PATH)
    os.makedirs(local_pickle_path, exist_ok=True)

    # build forward and inverted indexes
    res = index_building(sample_date, VOD_SAMPLING_DATA_PREDICTION_PARQUET_PATH, cols, targeting_idx_list, value_idx_list)

    with open(f'{local_pickle_path}/{sample_date}.pkl', 'wb') as f:
        pickle.dump(res, f)
    os.system(f"aws s3 cp {local_pickle_path}/{sample_date}.pkl {VOD_BITMAP_PICKLE_PATH}")

    jstr = dump_index_to_json(res)
    with open(f'{local_pickle_path}/{sample_date}.json', 'w') as f:
        f.write(jstr)
    os.system(f"aws s3 cp {local_pickle_path}/{sample_date}.json {VOD_BITMAP_JSON_PATH}")

    slack_notification(topic=SLACK_NOTIFICATION_TOPIC, region=REGION,
                       message=f"gec inventory index building on {sys.argv[1]} is done.")

# for offline data backfilling
# local_pickle_path = f'sample_data_model_300'
# # get the targeting col dict, list and non-targeting col list
# # cols, targeting_idx_list, value_idx_list = get_targeting_cols("2023-08-22", VOD_SAMPLING_DATA_PREDICTION_PARQUET_PATH)
# # os.makedirs(local_pickle_path, exist_ok=True)
# os.system(f"aws s3 sync {VOD_BITMAP_PICKLE_PATH} {local_pickle_path}/")
# for sample_date in get_date_list("2023-08-22", 300):
#     print(sample_date)
#     # res = index_building(sample_date, VOD_SAMPLING_DATA_PREDICTION_PARQUET_PATH, cols, targeting_idx_list,
#     #                      value_idx_list)
#     with open(f'{local_pickle_path}/{sample_date}.pkl', 'rb') as f:
#         res = pickle.load(f)
#     jstr = dump_index_to_json(res)
#     with open(f'{local_pickle_path}/{sample_date}.json', 'w') as f:
#         f.write(jstr)
#     os.system(f"aws s3 cp {local_pickle_path}/{sample_date}.json {VOD_BITMAP_JSON_PATH}")

