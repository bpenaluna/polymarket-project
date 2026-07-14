# Polymarket Project

A Kafka + Spark Structured Streaming pipeline that ingests Polymarket data, parses it, and writes it to Delta tables.

## Prerequisites

- Docker and Docker Compose installed

## How to Run

1. Clone the repo:
   ```bash
   git clone https://github.com/bpenaluna/polymarket-project.git
   cd polymarket-project
   ```

2. Build and start all services:
   ```bash
   docker compose up --build
   ```

   This spins up three services:
   - **kafka** — a single-node Kafka broker (KRaft mode, no Zookeeper) on port `9092`
   - **pm-producer** — polls the Polymarket API every 30s and publishes active market data to the `topicBTCpm` Kafka topic
   - **spark-app** — runs `stream-processor.py` via `spark-submit`, consuming from Kafka, parsing the JSON payloads, and writing the results out as Delta tables under `./data`

3. Data and checkpoints persist to your local machine via the mounted volumes:
   - `./data` — output Delta tables (`pm_data`, `cg_data`)
   - `./checkpoints` — Spark Structured Streaming checkpoints

4. Stop everything with:
   ```bash
   docker compose down
   ```

## Viewing the Data

`view_data.py` reads the `pm_data` Delta table and prints it out. Run it from within the `spark-app` container (or any environment with matching PySpark + Delta dependencies):

```bash
docker compose exec spark-app spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,io.delta:delta-spark_2.12:3.3.0 view_data.py
```

## Notes

- `stream-processor.py` expects data on both `topicBTCpm` (Polymarket) and `topicBTCcg` (CoinGecko), but only the Polymarket producer is currently included — a CoinGecko producer would need to be added to populate `cg_data`.
