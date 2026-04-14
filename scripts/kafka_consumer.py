from kafka import KafkaConsumer
import json
from collections import defaultdict

consumer = KafkaConsumer(
    'movie_events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Consumer started...")

event_count = defaultdict(int)

for message in consumer:
    event = message.value

    movie = event["movie_title"]
    event_type = event["event"]

    # Count events per movie
    key = f"{movie} ({event_type})"
    event_count[key] += 1

    print("\nLIVE ANALYTICS DASHBOARD")
    for k, v in list(event_count.items())[-5:]:
        print(f"{k}: {v}")