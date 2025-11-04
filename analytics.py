import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, avg, stddev, lag, lit, round, when, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
import traceback
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("CryptoMetricsCalculator") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# === PostgreSQL Connection Config ===
jdbc_url = "jdbc:postgresql://localhost:5432/crypto_metrics"
main_table = "crypto_table"
gainers_table = "top_5_gainers"
losers_table = "top_5_losers"
db_properties = {
    "user": "postgres",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

while True:
    try:
        print("\n=== Running Spark Job ===")

        # Load recent parquet files and drop unused columns
        df = spark.read.parquet(
            "file:///C:/Users/pc/Desktop/WORKSPACES/Marketflow-Analysis/dataframes"
        ).drop(
            "market_cap", "total_volume", "high_24h", "low_24h", "last_updated"
        )

        # Explicit Type Casting
        df = df.withColumn("timestamp", to_timestamp("timestamp")) \
               .withColumn("price", col("price").cast(DoubleType()))

        # Filter data from last 7 minutes 
        latest_data = df.filter(col("timestamp") >= F.current_timestamp() - F.expr("INTERVAL 7 MINUTES"))
        row_count = latest_data.count()

        if row_count == 0:
            print("No data found in the last 7 minutes. Skipping this cycle.")
        else:
            # Define window partition 
            coin_window = Window.partitionBy("id").orderBy("timestamp")

            # Price Changes (1 min, 5 min) with null guards
            latest_data = latest_data \
                .withColumn("price_1min_ago", lag("price", 2).over(coin_window)) \
                .withColumn("price_5min_ago", lag("price", 10).over(coin_window)) \
                .withColumn(
                    "change_1min",
                    when(
                        col("price_1min_ago").isNull(), None
                    ).otherwise(
                        round((col("price") - col("price_1min_ago")) / col("price_1min_ago") * 100, 2)
                    )
                ) \
                .withColumn(
                    "change_5min",
                    when(
                        col("price_5min_ago").isNull(), None
                    ).otherwise(
                        round((col("price") - col("price_5min_ago")) / col("price_5min_ago") * 100, 2)
                    )
                ) \
                .drop("price_1min_ago", "price_5min_ago")

            # SMA and EMA (rolling windows) 
            latest_data = latest_data \
                .withColumn("SMA", avg("price").over(coin_window.rowsBetween(-4, 0))) \
                .withColumn("EMA", avg("price").over(coin_window.rowsBetween(-2, 0)))

            #  Volatility 
            latest_data = latest_data \
                .withColumn("volatility", stddev("price").over(coin_window.rowsBetween(-4, 0)))

            # Gainers and Losers with global ranking 
            # Get only the latest record per coin
            latest_per_coin = latest_data.withColumn("row_num", row_number()\
                            .over(Window.partitionBy("id").orderBy(col("timestamp")\
                            .desc()))).filter(col("row_num") == 1).drop("row_num")         
            gain_df = latest_per_coin \
    .filter(col("change_5min").isNotNull()) \
    .withColumn("rank", row_number().over(Window.orderBy(col("change_5min").desc()))) \
    .filter(col("rank") <= 5) \
    .select("rank", "id", "symbol")

# Top 5 Losers (distinct coins, based on latest data)
            loss_df = latest_per_coin \
    .filter(col("change_5min").isNotNull()) \
    .withColumn("rank", row_number().over(Window.orderBy(col("change_5min").asc()))) \
    .filter(col("rank") <= 5) \
    .select("rank", "id", "symbol")

            # === Show Output ===
            print("\n=== All Metrics (Latest Snapshot) ===")
            latest_data.select(
                "timestamp", "id", "symbol", "price",
                "change_1min", "change_5min", "SMA", "EMA", "volatility"
            ).orderBy("timestamp").show(truncate=False)

            print("=== Top 5 Gainers (5 min change) ===")
            gain_df.show(truncate=False)

            print("=== Top 5 Losers (5 min change) ===")
            loss_df.show(truncate=False)

            # Write to PostgreSQL 
            latest_data.write.jdbc(
                url=jdbc_url,
                table=main_table,
                mode="append",
                properties=db_properties
            )

            gain_df.write.jdbc(
                url=jdbc_url,
                table=gainers_table,
                mode="overwrite",
                properties=db_properties
            )

            loss_df.write.jdbc(
                url=jdbc_url,
                table=losers_table,
                mode="overwrite",
                properties=db_properties
            )

            print("Data written to PostgreSQL (all 3 tables).")

        
        spark.catalog.clearCache()

    except Exception as e:
        print(f"Error in Spark job: {e}")
        traceback.print_exc()

    # Sleep
    total_seconds = 360
    print("Waiting for next run:")
    for remaining in range(total_seconds, 0, -1):
        mins, secs = divmod(remaining, 60)
        print(f"\rNext run in {mins:02}:{secs:02}", end="")
        time.sleep(1)
    print("\n")
