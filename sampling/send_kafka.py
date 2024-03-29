import json

from kafka import KafkaProducer
import pandas as pd
from tqdm import tqdm

# path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/final/all/cd=2023-05-25/p0.parquet'
path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/final/all/cd=2023-06-14/p0.parquet'
df = pd.read_parquet(path)

topic='load.adtech.inventory.forecast'
producer = KafkaProducer(
    bootstrap_servers='10.15.49.238:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

def generate(row):
    return {
        'tournamentId' : 112,
        'seasonId' : 222,
        'matchId' : 333,
        'adPlacement' : 'MIDROLL',
        'platform' : row.platform,
        'nccs': row.nccs,
        'ageBucket': row.age,
        'customCohort': row.custom_cohorts,
        'gender': row.gender,
        'devicePrice': row.device,
        'city': row.city,
        'state': row.state,
        'country': row.country,
        'inventory' : int(row.inventory),
        'reach' : int(row.reach),
        'language': int(row.language),
        'inventoryId' : '112_222',
        'version' : 'mlv6',
    }


allow_dict = {
    'country': ['in'],
    'platform': [''],
}

df2 = df[(df.inventory >= 1)&(df.reach >= 1)].reset_index()

flush_message_count = 1000
for i, row in tqdm(df2.iterrows()):
    msg = generate(row)
    producer.send(topic, value=msg)
    if (i + 1) % flush_message_count == 0:
        producer.flush()

producer.flush()
producer.close()

for col in df.columns[:-2]:
    print('-'*10)
    print(df[col].value_counts().sort_values(ascending=False)/len(df))

mul = 1
for col in df.columns[:-2]:
    t = len(set(df[col]))
    print(col, t)
    mul *= t
