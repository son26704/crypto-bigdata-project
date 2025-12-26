# spark-apps/batch/batch_processor.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, min, max, sum, count, stddev,
    date_trunc, lit, current_timestamp, to_timestamp
)
import sys

# Cấu hình Postgres
POSTGRES_JDBC_URL = "jdbc:postgresql://postgres.crypto-bigdata.svc.cluster.local:5432/cryptodb"
POSTGRES_PROPERTIES = {
    "user": "cryptouser",
    "password": "cryptopass123",
    "driver": "org.postgresql.Driver"
}

# Cấu hình HDFS (Đã sửa lại DNS ngắn gọn hơn cho K8s)
# Nếu lỗi vẫn xảy ra, hãy thử dùng IP của Namenode pod
HDFS_BASE_PATH = "hdfs://namenode-0.namenode.crypto-bigdata.svc.cluster.local:9000/crypto-data"

def create_spark_session():
    return SparkSession.builder \
        .appName("CryptoBatchProcessor") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .config("spark.hadoop.dfs.datanode.use.datanode.hostname", "true") \
        .getOrCreate()

def read_data(spark):
    print(">>> Reading data from Realtime Prices...")
    # Đọc toàn bộ bảng (trong thực tế nên filter theo ngày để nhẹ hơn)
    df = spark.read.jdbc(
        url=POSTGRES_JDBC_URL,
        table="realtime_prices",
        properties=POSTGRES_PROPERTIES
    )
    # Ép kiểu timestamp cho chắc chắn
    return df.withColumn("timestamp", to_timestamp(col("timestamp")))

def process_hourly(df):
    print(">>> Processing Hourly Stats...")
    # Group by theo giờ trọn vẹn (yyyy-MM-dd HH:00:00)
    return df.groupBy(
        "symbol",
        date_trunc("hour", col("timestamp")).alias("hour_timestamp")
    ).agg(
        min("price").alias("low_price"),
        max("price").alias("high_price"),
        avg("price").alias("avg_price"),
        # Trong batch job, open/close chính xác cần window function phức tạp.
        # Ở đây ta dùng min/max timestamp (xấp xỉ) hoặc first/last
        # Để đơn giản cho project, tạm dùng min/max price làm range
        min("price").alias("open_price"), 
        max("price").alias("close_price"),
        sum("volume_24h").alias("total_volume"),
        avg("volume_24h").alias("avg_volume"),
        avg("market_cap").alias("avg_market_cap"),
        stddev("price").alias("price_volatility"),
        count("*").alias("record_count")
    ).withColumn("created_at", current_timestamp())

def process_daily(df):
    print(">>> Processing Daily Stats...")
    return df.groupBy(
        "symbol",
        date_trunc("day", col("timestamp")).alias("day_timestamp")
    ).agg(
        min("price").alias("low_price"),
        max("price").alias("high_price"),
        avg("price").alias("avg_price"),
        # Giả lập open/close
        min("price").alias("open_price"),
        max("price").alias("close_price"),
        sum("volume_24h").alias("total_volume"),
        max("volume_24h").alias("peak_volume"),
        stddev("price").alias("volatility"),
        count("*").alias("transaction_count")
    ).withColumn(
        "price_change_percent",
        ((col("close_price") - col("open_price")) / col("open_price") * 100)
    ).withColumn("created_at", current_timestamp())

def write_to_db(df, table):
    print(f">>> Writing to DB Table: {table}...")
    try:
        df.write.jdbc(
            url=POSTGRES_JDBC_URL,
            table=table,
            mode="append", # Append dữ liệu mới
            properties=POSTGRES_PROPERTIES
        )
        print("✓ Success")
    except Exception as e:
        print(f"✗ Error writing DB: {e}")

def write_to_hdfs(df, path):
    print(f">>> Writing to HDFS: {path}...")
    try:
        # Dùng Coalesce(1) để gom thành 1 file Parquet duy nhất (Dễ quản lý)
        df.coalesce(1).write.mode("overwrite").parquet(path)
        print("✓ Success")
    except Exception as e:
        print(f"✗ Error writing HDFS: {e}")

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    raw_df = read_data(spark)
    
    if raw_df.count() == 0:
        print("No data found!")
        return

    # 1. Hourly
    hourly_df = process_hourly(raw_df)
    hourly_df.show(5)
    write_to_db(hourly_df, "hourly_stats")
    write_to_hdfs(hourly_df, f"{HDFS_BASE_PATH}/batch/hourly")

    # 2. Daily
    daily_df = process_daily(raw_df)
    daily_df.show(5)
    write_to_db(daily_df, "daily_stats")
    write_to_hdfs(daily_df, f"{HDFS_BASE_PATH}/batch/daily")

    spark.stop()

if __name__ == "__main__":
    main()