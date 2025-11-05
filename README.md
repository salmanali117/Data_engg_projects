# Data_engg_project_AWS
Designing and Implementing pipelines using AWS platforms.


# Architecture:
CoinGecko API → Kafka Producer → Kafka Topic → PySpark Consumer → DataFrames (Parquet)
           ↓                                                ↓
     PostgreSQL (Analytics Tables) ← Spark Transformations (SMA, EMA, Volatility)
