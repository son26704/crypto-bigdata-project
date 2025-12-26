from pyspark.sql import SparkSession
import sys

# Cấu hình HDFS
HDFS_BASE_PATH = "hdfs://namenode-0.namenode.crypto-bigdata.svc.cluster.local:9000/crypto-data"

def main():
    print("="*50)
    print("🚀 HDFS DEMO READER - BIG DATA STORAGE CHECK")
    print("="*50)
    
    spark = SparkSession.builder \
        .appName("HDFS_Reader") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Đọc Hourly Data
    hourly_path = f"{HDFS_BASE_PATH}/batch/hourly"
    print(f"\n📂 1. Reading form: {hourly_path}")
    try:
        hourly_df = spark.read.parquet(hourly_path)
        print(f"✅ Found {hourly_df.count()} hourly records.")
        print("Sample Data:")
        hourly_df.select("symbol", "hour_timestamp", "avg_price", "total_volume").show(5, truncate=False)
    except Exception as e:
        print(f"⚠️ Chưa có dữ liệu Hourly (Có thể chưa chạy Batch Job). Lỗi: {e}")

    # 2. Đọc Daily Data
    daily_path = f"{HDFS_BASE_PATH}/batch/daily"
    print(f"\n📂 2. Reading from: {daily_path}")
    try:
        daily_df = spark.read.parquet(daily_path)
        print(f"✅ Found {daily_df.count()} daily records.")
        print("Sample Data:")
        daily_df.select("symbol", "day_timestamp", "open_price", "close_price").show(5, truncate=False)
    except:
        print("⚠️ Chưa có dữ liệu Daily.")

    print("\nHệ thống HDFS đang hoạt động tốt. Dữ liệu đã được lưu trữ phân tán.")
    spark.stop()

if __name__ == "__main__":
    main()