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
CHECKPOINT_LOCATION = "/tmp/checkpoint_realtime_v2" # Đổi folder checkpoint để tránh lỗi metadata cũ

sys.path.append('/opt/spark-apps/common')
try:
    from postgres_config import POSTGRES_JDBC_URL, POSTGRES_PROPERTIES
except ImportError:
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
        StructField("price", DoubleType(), True),
        StructField("market_cap", DoubleType(), True),
        StructField("volume_24h", DoubleType(), True),
        StructField("price_change_24h", DoubleType(), True),
        StructField("last_updated", LongType(), True),
        StructField("timestamp", StringType(), True)
    ])

# --- CORE LOGIC ---
def process_batch(batch_df, batch_id):
    batch_df.cache()
    record_count = batch_df.count()
    print(f"--- Processing Batch ID: {batch_id} with {record_count} records ---")

    if record_count == 0:
        batch_df.unpersist()
        return

    window_spec = Window.partitionBy("symbol").orderBy("timestamp")
    
    # 1. Tính toán Moving Average
    processed_df = batch_df \
        .withColumn("ma_5min", avg("price").over(window_spec.rowsBetween(-4, 0))) \
        .withColumn("ma_15min", avg("price").over(window_spec.rowsBetween(-14, 0))) \
        .withColumn("ma_1hour", avg("price").over(window_spec.rowsBetween(-59, 0))) \
        .withColumn("price_change_percentage_24h", col("price_change_24h")) \
        .withColumn("created_at", current_timestamp())

    # 2. Detect Alerts
    alerts_df = processed_df.filter(abs(col("price_change_percentage_24h")) > 5) \
        .withColumn("alert_type", lit("VOLATILITY_WARNING")) \
        .withColumn("message", concat(lit("High volatility detected for "), col("symbol"))) \
        .select(
            "symbol", "alert_type", "message", 
            col("price").alias("price_after"),
            lit(0).alias("price_before"),
            col("price_change_percentage_24h").alias("change_percentage"),
            col("volume_24h").alias("volume"),
            col("timestamp").cast("timestamp"),
            "created_at"
        )

    # 3. Ghi Realtime Prices
    try:
        write_df = processed_df.select(
            "symbol", "name", "price", "volume_24h", "market_cap", 
            "price_change_24h", "price_change_percentage_24h", 
            "ma_5min", "ma_15min", "ma_1hour", 
            col("timestamp").cast("timestamp"), "created_at"
        )
        write_df.write.jdbc(
            url=POSTGRES_JDBC_URL, table="realtime_prices", mode="append", properties=POSTGRES_PROPERTIES
        )
        print("✓ Wrote prices to DB")
    except Exception as e:
        print(f"✗ Error writing prices: {e}")

    # 4. Ghi Alerts
    if alerts_df.count() > 0:
        try:
            alerts_df.write.jdbc(
                url=POSTGRES_JDBC_URL, table="alerts", mode="append", properties=POSTGRES_PROPERTIES
            )
            print(f"✓ Wrote alerts to DB")
        except Exception as e:
            print(f"✗ Error writing alerts: {e}")

    batch_df.unpersist()

# --- MAIN ---
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