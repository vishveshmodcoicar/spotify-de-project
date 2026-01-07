import os
import json
import time
import uuid
import random
from faker import Faker
from datetime import datetime
from confluent_kafka import Producer
from dotenv import load_dotenv

# --------------------------------------------
# Load environment variables
# --------------------------------------------
load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "spotify-events")
USER_COUNT = int(os.getenv("USER_COUNT", 20))
EVENT_INTERVAL_SECONDS = int(os.getenv("EVENT_INTERVAL_SECONDS", 1))

fake = Faker()

# --------------------------------------------
# Kafka Producer (confluent-kafka)
# --------------------------------------------
producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "linger.ms": 10,
    "acks": "all"
})

# --------------------------------------------
# Stable Song / Artist Definitions
# --------------------------------------------
song_artist_pairs = [
    {"artist": "The Weeknd", "song": "Blinding Lights"},
    {"artist": "Dua Lipa", "song": "Levitating"},
    {"artist": "Drake", "song": "God's Plan"},
    {"artist": "Taylor Swift", "song": "Love Story"},
    {"artist": "Ed Sheeran", "song": "Shape of You"},
    {"artist": "Kanye West", "song": "Stronger"}
]

for pair in song_artist_pairs:
    name_for_uuid = f"{pair['artist']}::{pair['song']}"
    pair["song_id"] = str(uuid.uuid5(uuid.NAMESPACE_DNS, name_for_uuid))

devices = ["mobile", "desktop", "web"]
countries = ["US", "UK", "CA", "AU", "IN", "DE"]
event_types = ["play", "pause", "skip", "add_to_playlist"]

# Generate random users
user_ids = [str(uuid.uuid4()) for _ in range(USER_COUNT)]

# --------------------------------------------
# Event Generator
# --------------------------------------------
def generate_event():
    pair = random.choice(song_artist_pairs)
    user_id = random.choice(user_ids)
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "song_id": pair["song_id"],
        "artist_name": pair["artist"],
        "song_name": pair["song"],
        "event_type": random.choice(event_types),
        "device_type": random.choice(devices),
        "country": random.choice(countries),
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }

# --------------------------------------------
# Delivery Report Callback (optional but useful)
# --------------------------------------------
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered to {msg.topic()} "
            f"[partition {msg.partition()}] "
            f"@ offset {msg.offset()}"
        )

# --------------------------------------------
# Main Loop
# --------------------------------------------
if __name__ == "__main__":
    print("Starting Spotify data simulator...")
    print(f"Using {len(song_artist_pairs)} songs and {len(user_ids)} users.")
    print(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")

    for p in song_artist_pairs:
        print(f"{p['song']} — {p['artist']} -> song_id={p['song_id']}")

    try:
        while True:
            event = generate_event()

            producer.produce(
                topic=KAFKA_TOPIC,
                value=json.dumps(event).encode("utf-8"),
                on_delivery=delivery_report
            )

            # Serve delivery callbacks
            producer.poll(0)

            print(
                f"Produced event: {event['event_type']} - "
                f"{event['song_name']} by {event['artist_name']} "
                f"(user {event['user_id']})"
            )

            time.sleep(EVENT_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("Stopping producer...")

    finally:
        producer.flush()
        print("Producer closed cleanly.")