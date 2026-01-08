Spotify Real-Time Data Engineering Pipeline (End-to-End)

1. System Architecture and Functional Overview

Architecture Diagram

graph LR
    A[Spotify Event Simulator<br/>Python] --> B[Kafka Broker]
    B --> C[Kafka Consumer<br/>Confluent Kafka]
    C --> D[MinIO Object Storage<br/>Bronze Layer]
    D --> E[dbt Transformations]
    E --> F[Silver Layer]
    F --> G[Gold Layer]
    G --> H[Snowflake / BI / Dashboards]
    E -->|Orchestrated by| I[Airflow]

This diagram represents the functional flow of data, not just the tools involved. Each component is decoupled, scalable, and replaceable, which is a core principle of modern data platforms.

This project is designed as a production-style real-time data engineering system inspired by how large-scale music streaming platforms process user activity data. The architecture is intentionally modular so that each component can scale, fail, and evolve independently.

At a functional level, the system performs four core tasks:
	1.	Event Generation – Simulate realistic Spotify-style user listening behavior.
	2.	Real-Time Streaming – Transport events reliably and at scale.
	3.	Data Lake Ingestion – Persist raw and processed data in a structured, queryable format.
	4.	Analytics & Transformation – Enable analytical queries and user-facing insights.

The system is structured around the following layers:

User Events → Kafka → Bronze Layer → Silver Layer → Gold Layer → Analytics / Dashboards

Each layer has a clearly defined responsibility, which mirrors how modern data platforms are designed in production.

⸻

2. Core Technologies and Why They Are Used

Kafka (Real-Time Event Backbone)

Kafka acts as the central nervous system of the platform.

Functionally, Kafka:
	•	Accepts high-throughput event streams from producers
	•	Decouples data producers from downstream consumers
	•	Stores events durably for replay and recovery
	•	Enables multiple consumers to process the same data independently

Kafka is used here because real-world streaming platforms never send data directly to databases. Instead, they rely on an event log that allows reprocessing, scaling, and fault tolerance.

In this project:
	•	A Python-based simulator produces events into Kafka topics
	•	Kafka guarantees ordering and durability
	•	Consumers read events at their own pace using offsets

⸻

Confluent Kafka Client (Python)

The Confluent Kafka Python client is used instead of kafka-python due to:
	•	Better compatibility with Kafka 3.x
	•	Stable protocol handling
	•	Production-grade performance

This decision was made after encountering broker handshake and API version issues on macOS with newer Python versions. Using Confluent Kafka ensures consistent behavior across environments.

⸻

MinIO (Object Storage / Data Lake)

MinIO provides S3-compatible object storage and represents the data lake layer.

Functionally, MinIO:
	•	Stores immutable event data
	•	Acts as the source of truth for analytics
	•	Supports partitioned layouts for efficient querying

MinIO is chosen because it mirrors how cloud data lakes (such as S3) are used in real production systems, while remaining lightweight and local-friendly.

⸻

Snowflake (Analytical Data Warehouse)

Snowflake represents the analytics execution layer.

Its role in the architecture:
	•	Query large volumes of data efficiently
	•	Separate compute from storage
	•	Power dashboards and analytical workloads

In this project, Snowflake is conceptually used as the warehouse querying Silver and Gold datasets produced from the data lake.

⸻

Airflow (Orchestration Layer)

Airflow is responsible for workflow orchestration.

Functionally, Airflow:
	•	Schedules batch jobs
	•	Manages dependencies between pipeline stages
	•	Retries failed tasks
	•	Provides observability into pipeline health

In this architecture, Airflow would orchestrate:
	•	Bronze → Silver transformations
	•	Silver → Gold aggregations
	•	Periodic backfills

⸻

dbt (Transformation & Modeling Layer)

dbt is used to:
	•	Transform raw data into analytics-ready tables
	•	Enforce data quality via tests
	•	Manage transformations using version-controlled SQL

dbt ensures that business logic is explicit, testable, and reproducible, which is critical for analytics reliability.

⸻

3. Data Layer Design (Bronze, Silver, Gold)

Bronze Layer (Raw Ingestion)

Purpose:
	•	Store raw, immutable event data exactly as received
	•	Preserve full historical fidelity
	•	Enable replay and reprocessing

Characteristics:
	•	Append-only
	•	Minimal transformations
	•	Partitioned by ingestion time

Example Bronze SQL Query:

SELECT
    event_id,
    user_id,
    song_id,
    artist_name,
    song_name,
    event_type,
    device_type,
    country,
    timestamp
FROM bronze.spotify_events
WHERE DATE(timestamp) = '2026-01-07';

Explanation:
This query retrieves raw events for a specific day. Analysts use Bronze data primarily for debugging, audits, and reprocessing rather than direct reporting.

⸻

Silver Layer (Cleaned & Standardized)

Purpose:
	•	Clean malformed data
	•	Normalize schemas
	•	Apply business logic
	•	Deduplicate events

Typical Transformations:
	•	Convert timestamps to proper types
	•	Standardize country codes
	•	Filter invalid events
	•	Enforce uniqueness constraints

Example Silver SQL Query:

SELECT
    event_id,
    user_id,
    song_id,
    artist_name,
    song_name,
    event_type,
    device_type,
    country,
    CAST(timestamp AS TIMESTAMP) AS event_timestamp
FROM silver.spotify_events_clean
WHERE event_type IN ('play', 'pause', 'skip');

Explanation:
This query operates on cleaned data, ensuring consistent types and validated event categories. The Silver layer is the primary input for analytics.

⸻

Gold Layer (Business Aggregations)

Purpose:
	•	Provide analytics-ready datasets
	•	Power dashboards and reports
	•	Optimize for query performance

Typical Aggregations:
	•	Plays per song
	•	User engagement metrics
	•	Top artists by region
	•	Time-based trends

Example Gold SQL Query:

SELECT
    song_name,
    artist_name,
    COUNT(*) AS total_plays
FROM gold.spotify_song_metrics
WHERE event_type = 'play'
GROUP BY song_name, artist_name
ORDER BY total_plays DESC
LIMIT 10;

Explanation:
This query produces a leaderboard of the most played songs. Gold tables are designed specifically for consumption by BI tools and applications.

⸻

4. End-to-End Workflow Summary
	1.	Events are generated in real time by a Python simulator
	2.	Kafka ingests and buffers these events
	3.	A Kafka consumer batches and writes events to MinIO (Bronze layer)
	4.	Airflow schedules transformation jobs
	5.	dbt transforms Bronze data into Silver and Gold models
	6.	Snowflake executes analytical queries on Gold tables
	7.	Dashboards and applications consume Gold datasets

⸻

5. Future Scope and Enhancements

Planned extensions of this project include:
	•	User authentication and account-based analytics
	•	Real-time user-to-user comparisons
	•	Streaming aggregations using Spark Structured Streaming
	•	Schema evolution and Parquet storage
	•	Exactly-once processing semantics
	•	Real-time dashboards and leaderboards

⸻

6. Project Initialization (End-to-End Setup)

This section describes how to bring the entire system up from a clean machine to a fully working real-time pipeline.

⸻

Step 1: Prerequisites

Ensure the following are installed:
	•	Docker Desktop (running)
	•	Conda or Miniconda
	•	Git

⸻

Step 2: Clone the Repository

git clone <repository-url>
cd spotify-de-project


⸻

Step 3: Create Python 3.10 Environment

Kafka client compatibility issues on macOS require Python 3.10.

conda create -n kafka310 python=3.10 -y
conda activate kafka310


⸻

Step 4: Install Python Dependencies

pip uninstall kafka-python -y
pip install confluent-kafka boto3 faker python-dotenv


⸻

Step 5: Environment Configuration

Create a .env file at the project root:

KAFKA_BOOTSTRAP_SERVERS=localhost:29092
KAFKA_TOPIC=spotify-events
KAFKA_GROUP_ID=spotify-minio-consumer

MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=spotify-events

BATCH_SIZE=10


⸻

Step 6: Start Infrastructure (Kafka + MinIO)

docker compose up -d

Validate services:
	•	Kafka: localhost:29092
	•	MinIO UI: http://localhost:9000

⸻

Step 7: Run the Kafka Producer

python simulator/producer.py

This starts generating Spotify-style events in real time.

⸻

Step 8: Run the Kafka → MinIO Consumer

python consumer/kafka_to_minio.py

You should now see JSON files appearing in MinIO under:

bronze/date=YYYY-MM-DD/hour=HH/


⸻

Step 9: Verify Data Flow
	•	Use MinIO UI to confirm files are being written
	•	Use Kafka CLI or logs to confirm offsets are advancing
	•	Restart consumer to verify offset persistence

⸻

7. Final Notes

This project is intentionally structured to resemble real-world data engineering systems:
	•	Event-driven ingestion
	•	Decoupled services
	•	Replayable raw data
	•	Layered data modeling

It is suitable for:
	•	Portfolio demonstration
	•	Interview walkthroughs
	•	Foundation for advanced analytics platforms

This project demonstrates how real-world data platforms are built:
	•	Event-first design
	•	Decoupled services
	•	Scalable storage
	•	Layered data modeling

It is designed to be both educational and extensible, serving as a strong foundation for production-grade data engineering systems.
