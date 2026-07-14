# Polymarket Project

A Kafka + Spark Structured Streaming pipeline that ingests data from https://polymarket.com/event/btc-updown-5m-1784061600, parses it, writes it to Delta tables and displays the data on a live dashboard.

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
   - **kafka** — a single-node Kafka broker (KRaft) on port `9092`
   - **pm-producer** — pulls the Polymarket API every 30s and publishes active market data to the `topicBTCpm` Kafka topic
   - **spark-app** — runs `stream-processor.py` via `spark-submit`, consuming from Kafka, parsing the JSON, and writing the results as Delta tables under `./data`
   - **dashboard** — a Dash app that reads the Delta tables and displays them as live-updating charts on port `8050`

3. Data and checkpoints persist to your local machine via the mounted volumes:
   - `./data` — output Delta tables (`pm_data`, `cg_data`)
   - `./checkpoints` — Spark Structured Streaming checkpoints

4. Stop everything with:
   ```bash
   docker compose down
   ```

## Dashboard

Once the stack is up, visit **http://localhost:8050** to view it. It shows two live-updating line charts, refreshed every 30 seconds:

- **Up/Down Outcome Prices Over Time** — Polymarket's prices for the Up and Down outcomes
- **Bitcoin Price** — CoinGecko's BTC/USD price over time, read from `cg_data`

It's built with **Dash** and **Plotly**, and reads the Delta tables directly using **DuckDB**'s `delta_scan`, so it doesn't need a JVM or PySpark to run. The `./data` folder is mounted into the dashboard container read-only, so it never writes to the tables Spark is producing.

## Viewing the Data

`view_data.py` reads the `pm_data` Delta table and prints it out. Run it from within the `spark-app` container (or any environment with matching PySpark + Delta dependencies):

```bash
docker compose exec spark-app spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,io.delta:delta-spark_2.12:3.3.0 view_data.py
```
