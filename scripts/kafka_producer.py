from kafka import KafkaProducer
import json
import pandas as pd
import time
import random

# Load dataset
df = pd.read_csv("data/wb_movies.csv")

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Producer started...")

while True:
    movie = df.sample(1).iloc[0]

    event = {
    "user_id": random.randint(1000, 9999),
    "movie_id": int(movie.get("id", 0)),
    "movie_title": movie.get("title", "Unknown"),
    "event": random.choice(["click", "watch", "like"]),
    "timestamp": time.time()
}

    producer.send("movie_events", value=event)
    print("Sent:", event)

    time.sleep(1)  # simulate real-time