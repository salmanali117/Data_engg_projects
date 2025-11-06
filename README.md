# Data_engg_project Real Time Streaming
Designing and Implementing pipelines using AWS platforms.

## Overview
This project implements a real-time streaming ETL pipeline to ingest, process, and analyze live cryptocurrency market data using Apache Kafka, PySpark Structured Streaming, and PostgreSQL.
It continuously fetches price data from the CoinGecko API, streams it via Kafka, performs transformations and analytics in PySpark, and loads processed metrics into a PostgreSQL database for visualization


## Architecture:
CoinGecko API → Kafka Producer → Kafka Topic → PySpark Consumer → DataFrames (Parquet)
                                                           ↓
     PostgreSQL (Analytics Tables) ← Spark Transformations (SMA, EMA, Volatility)


## Step by Step workflow:

### Data Ingestion (Producer Layer)

* A Python script continuously fetches live cryptocurrency data from the CoinGecko API every 30 seconds.

* Extracts key attributes such as coin id, symbol, price, market_cap, volume, high_24h, low_24h, and timestamp.

* Each batch is serialized as JSON and streamed into the Kafka topic crypto-prices acting as a real-time message queue.

### Real-Time Stream Processing (Consumer Layer)

* The kafka_streaming.py script initializes a Spark Structured Streaming job that consumes messages from the Kafka topic.

* Each JSON payload is parsed, flattened, and transformed into tabular structure using Spark SQL functions.

* The processed data is written every 30 seconds into Parquet files for efficient columnar storage, with checkpointing enabled to ensure fault tolerance and recovery.
  
### Batch Analytics & Metric Computation

* The analytics.py script reads the latest Parquet data and computes real-time analytical metrics such as:

   * Price Change (1-min & 5-min) – short-term performance indicators.

   * SMA (Simple Moving Average) – smooths out short-term price noise.

   * EMA (Exponential Moving Average) – prioritizes recent market trends.

   * Volatility – measures the degree of price fluctuation over time.

* Each coin’s latest data point is identified and ranked globally to determine the Top 5 Gainers and Top 5 Losers over the last 5 minutes.

### Data Storage & Integration

* The pipeline writes three tables into PostgreSQL:

  * crypto_table → All calculated metrics.

  * top_5_gainers → Highest 5 performers based on 5-min change.

  * top_5_losers → Lowest 5 performers based on 5-min change.

* Database schema and connection parameters are pre-configured for easy integration with BI tools such as Power BI or Looker Studio.

### Orchestration & Execution

* The entire pipeline can be orchestrated via Airflow DAG (crypto_dag.py) or run sequentially using command-line scripts.
