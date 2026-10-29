# Kafka to Snowflake Order Streaming

A real time pipeline that streams order events from an API through Apache Kafka into Snowflake, with at least once delivery and crash recovery.

## Architecture

API (FastAPI) → producer.py → Kafka topic `api_orders` (3 partitions) → consumer.py → Snowflake `financials.raw.api_orders_raw`

• **API**: generates live orders at `/orders/live` (1 to 5 new orders per call)
• **Producer**: polls the API every 2 seconds and sends each order to Kafka, keyed by `order_id`
• **Kafka**: stores events in 3 partitions, running in Docker (KRaft mode, no ZooKeeper)
• **Consumer**: reads in batches (50 events or 10 seconds), inserts into Snowflake, then commits offsets
• **Snowflake**: stores raw JSON in a VARIANT column with Kafka metadata (key, partition, offset, loaded_at)

## Tech Stack

Python, Apache Kafka 3.9 (Docker), confluent-kafka, Snowflake (key pair auth), FastAPI, uv

## Design Decisions

• **Keyed by order_id**: the same order always goes to the same partition, so its events stay in order
• **acks=all and idempotence**: the producer only counts an event as sent once Kafka has saved it, and retries never create duplicates
• **Manual commits after insert**: offsets are committed only after the Snowflake insert succeeds, giving at least once delivery. A crash can cause duplicates but never data loss, and duplicates are removed downstream by deduplicating on order_id
• **Batch inserts**: one insert per batch instead of per event, which is far cheaper in Snowflake
• **Kafka metadata stored**: partition and offset are saved with every row, so each row can be traced back to its exact event

## Setup

1. Start Kafka
```bash
docker compose up -d
```

2. Create the topic
```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic api_orders --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
```

3. Create the Snowflake table
```sql
create table if not exists financials.raw.api_orders_raw (
    data variant,
    kafka_key string,
    kafka_partition int,
    kafka_offset int,
    loaded_at timestamp_ntz
);
```

4. Create a `.env` file
```
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PRIVATE_KEY_PATH=~/.snowflake/rsa_key.p8
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
```

5. Install dependencies
```bash
uv sync
```

## Running

Run each in its own terminal:
```bash
uv run uvicorn api:app --reload      # in the API folder
uv run producer.py
uv run consumer.py
```

## Monitoring Consumer Lag

```bash
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group snowflake-loader
```

LAG = latest offset minus committed offset. A small, stable lag means the consumer is keeping up.

## Crash Recovery Test

1. Stopped the consumer while the producer kept running
2. Lag grew to around 30 events per partition, with events held safely in Kafka
3. Restarted the consumer
4. It resumed from the last committed offset, and lag dropped back to 0 to 4

Result: no data lost.

## Verify in Snowflake

```sql
select kafka_partition, count(*) from financials.raw.api_orders_raw group by 1;
```