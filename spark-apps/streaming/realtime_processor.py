# spark-apps/streaming/realtime_processor.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, abs, lit, concat, current_timestamp, from_json
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)
from pyspark.sql.window import Window
import sys

# --- CONFIGURATION ---
KAFKA_BOOTSTRAP_SERVERS = "kafka-0.kafka.crypto-bigdata.svc.cluster.local:9092"
KAFKA_TOPIC = "crypto-prices"
CHECKPOINT_LOCATION = "/tmp/checkpoint_realtime_v4"

sys.path.append('/opt/spark-apps/common')
try:
    from config import POSTGRES_JDBC_URL, POSTGRES_PROPERTIES
except ImportError:
    print("Could not load config file, using default hardcoded values")
    POSTGRES_JDBC_URL = "jdbc:postgresql://postgres.crypto-bigdata.svc.cluster.local:5432/cryptodb"
    POSTGRES_PROPERTIES = {
        "user": "cryptouser",
        "password": "cryptopass123",
        "driver": "org.postgresql.Driver"
    }

def create_spark_session():
    return SparkSession.builder \
        .appName("CryptoRealtimeProcessor") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.ui.showConsoleProgress", "false") \
        .getOrCreate()

def get_schema():
    return StructType([
        StructField("id", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("name", StringType(), True),
        StructField("image", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("market_cap", DoubleType(), True),
        StructField("volume_24h", DoubleType(), True),
        StructField("high_24h", DoubleType(), True),
        StructField("low_24h", DoubleType(), True),
        StructField("ath", DoubleType(), True),
        StructField("atl", DoubleType(), True),
        StructField("price_change_24h", DoubleType(), True),
        StructField("price_change_percentage_24h", DoubleType(), True),
        StructField("last_updated", LongType(), True),
        StructField("ingestion_timestamp", StringType(), True)
    ])

def process_batch(batch_df, batch_id):
    batch_df.cache()
    if batch_df.count() == 0:
        batch_df.unpersist()
        return

    print(f"--- Processing Batch {batch_id} ---")
    
    base_df = batch_df.withColumn("timestamp", col("ingestion_timestamp").cast("timestamp"))

    window_spec = Window.partitionBy("symbol").orderBy("timestamp")
    
    processed_df = base_df \
        .withColumn("ma_5min", avg("price").over(window_spec.rowsBetween(-4, 0))) \
        .withColumn("ma_15min", avg("price").over(window_spec.rowsBetween(-14, 0))) \
        .withColumn("ma_1hour", avg("price").over(window_spec.rowsBetween(-59, 0))) \
        .withColumn("created_at", current_timestamp())

    # Detect Alerts
    alerts_df = processed_df.filter(abs(col("price_change_percentage_24h")) > 5) \
        .withColumn("alert_type", lit("VOLATILITY_WARNING")) \
        .withColumn("message", concat(lit("High volatility detected for "), col("symbol"))) \
        .select(
            "symbol", "alert_type", "message", 
            col("price").alias("price_after"),
            col("price_change_percentage_24h").alias("change_percentage"),
            "timestamp", "created_at"
        )

    # Ghi Realtime Prices
    try:
        write_df = processed_df.select(
            "symbol", "name", "image", "price", "market_cap", "volume_24h",
            "high_24h", "low_24h", "ath", "atl",
            "price_change_24h", "price_change_percentage_24h",
            "ma_5min", "ma_15min", "ma_1hour",
            "timestamp", "created_at"
        )
        write_df.write.jdbc(url=POSTGRES_JDBC_URL, table="realtime_prices", mode="append", properties=POSTGRES_PROPERTIES)
        print("✓ Wrote prices to DB")
    except Exception as e:
        print(f"✗ Error prices: {e}")

    # Ghi Alerts
    if alerts_df.count() > 0:
        try:
            alerts_df.write.jdbc(url=POSTGRES_JDBC_URL, table="alerts", mode="append", properties=POSTGRES_PROPERTIES)
            print("✓ Wrote alerts")
        except Exception as e:
            print(f"✗ Error alerts: {e}")

    batch_df.unpersist()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    schema = get_schema()
    parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    query = parsed_df.writeStream \
        .foreachBatch(process_batch) \
        .trigger(processingTime="30 seconds") \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()