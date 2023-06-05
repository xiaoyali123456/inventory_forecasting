import json

from kafka import KafkaProducer
import pandas as pd

# s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/final/all/cd=2023-05-19/p0.parquet
df = pd.read_parquet('all.parquet')

topic='load.adtech.inventory.forecast'
producer = KafkaProducer(
    bootstrap_servers='10.15.49.238:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

def generate(row):
    return {
        'tournamentId' : 111,
        'seasonId' : 222,
        'matchId' : 333,
        'adPlacement' : 'MIDROLL',
        'platform' : row.platform,
        'nccs': row.nccs,
        'ageBucket': row.age,
        'customCohort': 'A_58290825',
        'gender': row.gender,
        'devicePrice': row.device,
        'city': row.city,
        'state': row.state,
        'country': row.country,
        'inventory' : int(row.inventory),
        'reach' : int(row.reach+0.5),
        'inventoryId' : '111_333',
        'version' : 'mlv1',
    }

# # single send
# future = producer.send(topic, template)
# meta = future.get(timeout=10)

flush_message_count = 1000
for i, row in tqdm(df.iterrows()):
    msg = generate(row)
    producer.send(topic, value=msg)
    if (i + 1) % flush_message_count == 0:
        producer.flush()

producer.flush()
producer.close()
