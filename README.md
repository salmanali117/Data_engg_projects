# Data_engg_project_AWS
Designing and Implementing pipelines using AWS platforms.

## Overview
This project implements a real-time streaming ETL pipeline to ingest, process, and analyze live cryptocurrency market data using Apache Kafka, PySpark Structured Streaming, and PostgreSQL.
It continuously fetches price data from the CoinGecko API, streams it via Kafka, performs transformations and analytics in PySpark, and loads processed metrics into a PostgreSQL database for visualization


## Architecture:
CoinGecko API → Kafka Producer → Kafka Topic → PySpark Consumer → DataFrames (Parquet)
           ↓                                                ↓
     PostgreSQL (Analytics Tables) ← Spark Transformations (SMA, EMA, Volatility)


## Step by Step procedure:
* The pipeline pulls live cryptocurrency prices from the CoinGecko API every 30 seconds.

The Python script fetches popular coins (like Bitcoin, Ethereum, etc.) and filters only useful details like price, volume, market cap, highs/lows, and timestamp.

Each batch of data is packaged into a JSON message and sent to a Kafka topic named crypto-prices, which acts like a real-time mailbox for incoming data.

The script "kafka_streaming.py" runs a Spark Structured Streaming job that takes data from the crypto-prices topic.

Each incoming Kafka message (JSON) is parsed and flattened, each coin’s details are extracted into individual rows.

Spark then writes this data every 30 seconds into Parquet files (columnar storage) inside your local dataframes folder, while maintaining checkpoints for recovery.

Reading the newly stored Parquet data using Spark and filtering recent data calculations are performed regarding:

  *Price change (1-min & 5-min) – measures short-term momentum.

  *SMA (Simple Moving Average) – smooths short-term fluctuations.

  *EMA (Exponential Moving Average) – gives more weight to recent prices.

  *Volatility – measures how much the price is fluctuating.

Based on these metrics, it finds the Top 5 gainers and Top 5 losers in the last 5 minutes and the results are written into PostgreSQL tables.

## Key Highlights

Designed a fault-tolerant, recoverable streaming pipeline using Kafka and Spark checkpoints.

Implemented real-time analytics on crypto price movements (SMA, EMA, volatility).

Integrated with PostgreSQL for downstream reporting and visualization.

Modular architecture allowing easy scaling and cloud deployment (GCP/AWS compatible).

End-to-end automation ready via Airflow DAG or shell scripts.
