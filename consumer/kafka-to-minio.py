import json
import os
from datetime import datetime
from confluent_kafka import Consumer
import boto3
from dotenv import load_dotenv

# ---------- Load environment variables ----------
load_dotenv()

# ---------- Configuration ----------
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "spotify-events")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "spotify-minio-consumer")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10))

# ---------- Connect to MinIO ----------
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

# Ensure bucket exists
try:
    s3.head_bucket(Bucket=MINIO_BUCKET)
    print(f"Bucket '{MINIO_BUCKET}' already exists.")
except Exception:
    s3.create_bucket(Bucket=MINIO_BUCKET)
    print(f"Created bucket '{MINIO_BUCKET}'.")

# ---------- Kafka Consumer ----------
consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True
})

consumer.subscribe([KAFKA_TOPIC])

print(f"Listening for Kafka events on topic '{KAFKA_TOPIC}'...")

batch = []

try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Kafka error: {msg.error()}")
            continue

        event = json.loads(msg.value().decode("utf-8"))
        batch.append(event)

        if len(batch) >= BATCH_SIZE:
            now = datetime.utcnow()
            date_path = now.strftime("date=%Y-%m-%d/hour=%H")
            file_name = f"spotify_events_{now.strftime('%Y-%m-%dT%H-%M-%S')}.json"
            file_path = f"bronze/{date_path}/{file_name}"

            json_data = "\n".join(json.dumps(e) for e in batch)

            s3.put_object(
                Bucket=MINIO_BUCKET,
                Key=file_path,
                Body=json_data.encode("utf-8")
            )

            print(f"Uploaded {len(batch)} events → s3://{MINIO_BUCKET}/{file_path}")
            batch.clear()

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    # Flush remaining events
    if batch:
        now = datetime.utcnow()
        file_name = f"spotify_events_final_{now.strftime('%Y-%m-%dT%H-%M-%S')}.json"
        s3.put_object(
            Bucket=MINIO_BUCKET,
            Key=f"bronze/final/{file_name}",
            Body="\n".join(json.dumps(e) for e in batch).encode("utf-8")
        )
        print(f"Flushed {len(batch)} remaining events.")

    consumer.close()