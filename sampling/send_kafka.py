from sampling.send_kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='10.15.49.238:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)
topic='load.adtech.inventory.forecast'

template = {
    'tournamentId' : 111,
    'seasonId' : 222,
    'matchId' : 333,
    'adPlacement' : 'MIDROLL',
    'platform' : 'IOS',
    # String nccs;
    # String ageBucket;
    # String customCohort;
    'gender': 'female',
    # Integer devicePrice;
    # String city;
    # String state;
    # String country;
    # String ssaiCohort;
    # String locationCluster;
    # String pincode;
    # String interest;
    # String customAudience;
    # String deviceBrand;
    # String deviceModel;
    # String primarySim;
    # String dataSim;
    # String appVersion;
    # String os;
    # String osVersion;
    # String subscriptionType;
    # String subscriptionTag;
    # String contentType;
    # String contentGenre;
    # String contentId;
    'inventory' : 100,
    'reach' : 10,
    'inventoryId' : '125_567',
    'version' : 'v1',
}

future = producer.send(topic, template)
meta = future.get(timeout=10)

flush_message_count = 1000
for i, row in enumerate(df_part):
    producer.send(topic, value=raw_bytes, key=row.dw_p_id.encode())
    if (i + 1) % flush_message_count == 0:
        producer.flush()

producer.flush()
producer.close()

