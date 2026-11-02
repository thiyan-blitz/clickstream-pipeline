# Real-Time Clickstream Pipeline

A streaming data pipeline that ingests simulated e-commerce events through
Redpanda (Kafka-compatible) into PostgreSQL, with idempotent loading and
event-time analytics.

## Architecture

```
┌─────────────┐    ┌──────────────────┐    ┌──────────────┐    ┌─────────────┐
│ producer.py │───▶│ Redpanda (Kafka) │───▶│ consumer.py  │───▶│ PostgreSQL  │
│ event gen   │    │ topic: events    │    │ batch + upsert│   │ events table│
│ (dups/late) │    │ 3 partitions     │    │ manual commit │   │ + analytics │
└─────────────┘    └──────────────────┘    └──────────────┘    └─────────────┘
                     key = user_id                              PK = event_id
```

## Key design decisions
- **Idempotent sink:** `event_id` is the primary key and inserts use
  `ON CONFLICT DO NOTHING`, so duplicate or replayed messages never create
  duplicate rows.
- **At-least-once delivery:** the consumer commits to Postgres first, then
  commits the Kafka offset. A crash in between causes a redelivery, which the
  idempotent insert absorbs.
- **Partitioning by user_id:** keeps each user's events ordered within a partition.
- **Event time vs ingestion time:** `event_time` (when it happened) and
  `ingested_at` (when it was stored) are separate columns, so late-arriving
  events are bucketed into the correct hour.
- **Batched writes:** `execute_values` inserts 100 rows per round trip.
- **Bad data handling:** malformed messages are counted and skipped, not fatal.

## Results
- Processed [2,299] messages ([2,200] unique + [99] duplicates); table held
  exactly the unique count, with zero duplicate rows.
- ~4.8% late events (target 5%) correctly attributed to their event-time hour.

## How to run
```bash
docker compose up -d
docker exec -it redpanda rpk topic create events -p 3
pip install -r requirements.txt
python consumer.py      # terminal 1
python producer.py      # terminal 2
```
Analytics queries are in `sql/analytics.sql`.

## What I'd do at 100x scale
- Replace the single consumer with a consumer group (one consumer per partition).
- Send malformed messages to a dead-letter topic.
- Use Spark Structured Streaming or Flink with watermarks for late-data handling.
- Land raw events in S3 (Parquet), partitioned by date, and load to Redshift.
- Partition the events table by `event_time` and add monitoring on consumer lag.