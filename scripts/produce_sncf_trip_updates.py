import time
import requests
import json
from kafka import KafkaProducer
from google.transit import gtfs_realtime_pb2

# Configuration Kafka
producer = KafkaProducer(
    bootstrap_servers='projet-trafic-kafka-1:9092',
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

TOPIC = 'sncf-trip-updates'
URL = 'https://proxy.transport.data.gouv.fr/resource/sncf-all-gtfs-rt-trip-updates'

def fetch_trip_updates():
    feed = gtfs_realtime_pb2.FeedMessage()
    response = requests.get(URL)
    feed.ParseFromString(response.content)

    records = []
    for entity in feed.entity:
        if entity.HasField('trip_update'):
            record = {
                "trip_id": entity.trip_update.trip.trip_id,
                "start_date": entity.trip_update.trip.start_date,
                "updates": []
            }
            for update in entity.trip_update.stop_time_update:
                record["updates"].append({
                    "stop_id": update.stop_id,
                    "arrival": update.arrival.time if update.HasField('arrival') else None,
                    "departure": update.departure.time if update.HasField('departure') else None
                })
            records.append(record)
    return records

while True:
    print("Fetching GTFS-RT trip updates...")
    data = fetch_trip_updates()
    for d in data:
        producer.send(TOPIC, d)
        print(" Sent:", d)
    time.sleep(60)  # 1 minute d’intervalle
