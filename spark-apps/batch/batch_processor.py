# spark-apps/batch/batch_processor.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, min, max, sum, count, stddev,
    date_trunc, current_timestamp, to_timestamp, lit, to_date
)
from datetime import timedelta
from typing import List

POSTGRES_JDBC_URL = "jdbc:postgresql://postgres.crypto-bigdata.svc.cluster.local:5432/cryptodb"
POSTGRES_PROPERTIES = {
    "user": "cryptouser",
    "password": "cryptopass123",
    "driver": "org.postgresql.Driver"
}

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
    df = spark.read.jdbc(
        url=POSTGRES_JDBC_URL,
        table="realtime_prices",
        properties=POSTGRES_PROPERTIES
    )
    return df.withColumn("timestamp", to_timestamp(col("timestamp")))

def execute_sql(spark, sql: str):
    jvm = spark._sc._gateway.jvm

    # Load driver bằng context classloader (thấy jar từ --packages)
    cl = jvm.java.lang.Thread.currentThread().getContextClassLoader()
    jvm.java.lang.Class.forName(POSTGRES_PROPERTIES["driver"], True, cl)

    # Tạo driver instance và connect trực tiếp (không qua DriverManager)
    driver = jvm.org.postgresql.Driver()

    props = jvm.java.util.Properties()
    props.setProperty("user", POSTGRES_PROPERTIES["user"])
    props.setProperty("password", POSTGRES_PROPERTIES["password"])

    conn = driver.connect(POSTGRES_JDBC_URL, props)
    if conn is None:
        raise Exception("PostgreSQL driver.connect() returned None (driver not suitable / classpath issue)")

    try:
        stmt = conn.createStatement()
        stmt.execute(sql)
        stmt.close()
    finally:
        conn.close()

def ensure_state_table(spark):
    sql = """
    CREATE TABLE IF NOT EXISTS batch_job_state (
      job_name TEXT PRIMARY KEY,
      last_processed_at TIMESTAMPTZ
    );
    """
    execute_sql(spark, sql)


def get_last_processed(spark, job_name: str):
    q = f"(SELECT last_processed_at FROM batch_job_state WHERE job_name = '{job_name}') AS t"
    df = spark.read.jdbc(url=POSTGRES_JDBC_URL, table=q, properties=POSTGRES_PROPERTIES)
    rows = df.collect()
    if not rows:
        return None
    return rows[0]["last_processed_at"]


def set_last_processed(spark, job_name: str, ts):
    # ts là python datetime (Spark collect ra)
    sql = f"""
    INSERT INTO batch_job_state(job_name, last_processed_at)
    VALUES ('{job_name}', '{ts.isoformat()}')
    ON CONFLICT (job_name) DO UPDATE
    SET last_processed_at = EXCLUDED.last_processed_at;
    """
    execute_sql(spark, sql)


# ---------- Processors ----------
def process_hourly(df):
    print(">>> Processing Hourly Stats...")
    return df.groupBy(
        "symbol",
        date_trunc("hour", col("timestamp")).alias("hour_timestamp")
    ).agg(
        min("price").alias("low_price"),
        max("price").alias("high_price"),
        avg("price").alias("avg_price"),
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
        # daily key nên là DATE để đúng unique constraint (đỡ lệch timezone/ts)
        to_date(date_trunc("day", col("timestamp"))).alias("day_timestamp")
    ).agg(
        min("price").alias("low_price"),
        max("price").alias("high_price"),
        avg("price").alias("avg_price"),
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


# ---------- UPSERT via staging ----------
def upsert_to_db(spark, df, target_table: str, key_cols: List[str]):
    """
    1) write df -> staging (overwrite)
    2) INSERT ... SELECT ... ON CONFLICT (keys) DO UPDATE ...
    3) drop staging
    """
    print(f">>> UPSERT to DB Table: {target_table}...")
    staging = f"{target_table}__staging"

    # 1) staging overwrite (tạo bảng staging mới mỗi lần)
    df.write.jdbc(
        url=POSTGRES_JDBC_URL,
        table=staging,
        mode="overwrite",
        properties=POSTGRES_PROPERTIES
    )

    cols = df.columns
    q_cols = ", ".join([f'"{c}"' for c in cols])
    q_keys = ", ".join([f'"{k}"' for k in key_cols])

    # update tất cả cột không nằm trong key
    non_keys = [c for c in cols if c not in key_cols]
    set_clause = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in non_keys])

    sql = f"""
    INSERT INTO "{target_table}" ({q_cols})
    SELECT {q_cols} FROM "{staging}"
    ON CONFLICT ({q_keys}) DO UPDATE
    SET {set_clause};
    """

    try:
        execute_sql(spark, sql)
        print("✓ Success")
    except Exception as e:
        print(f"✗ Error upserting DB: {e}")
        raise
    finally:
        # dọn staging
        try:
            execute_sql(spark, f'DROP TABLE IF EXISTS "{staging}";')
        except Exception:
            pass


def write_to_hdfs(df, path):
    print(f">>> Writing to HDFS: {path}...")
    try:
        df.coalesce(1).write.mode("overwrite").parquet(path)
        print("✓ Success")
    except Exception as e:
        print(f"✗ Error writing HDFS: {e}")


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    ensure_state_table(spark)

    raw_df = read_data(spark)
    if raw_df.count() == 0:
        print("No data found!")
        return

    # max timestamp hiện có (mốc tiến độ)
    max_ts = raw_df.agg(max(col("timestamp")).alias("mx")).collect()[0]["mx"]

    # ===== Hourly incremental window =====
    last_hourly = get_last_processed(spark, "hourly")
    # lookback để cập nhật các giờ gần đây (late data)
    hourly_start = (last_hourly - timedelta(hours=2)) if last_hourly else None
    hourly_src = raw_df if hourly_start is None else raw_df.filter(col("timestamp") >= lit(hourly_start))

    hourly_df = process_hourly(hourly_src)
    hourly_df.show(5)

    upsert_to_db(spark, hourly_df, "hourly_stats", key_cols=["symbol", "hour_timestamp"])
    write_to_hdfs(hourly_df, f"{HDFS_BASE_PATH}/batch/hourly")

    set_last_processed(spark, "hourly", max_ts)

    # ===== Daily incremental window =====
    last_daily = get_last_processed(spark, "daily")
    daily_start = (last_daily - timedelta(days=2)) if last_daily else None
    daily_src = raw_df if daily_start is None else raw_df.filter(col("timestamp") >= lit(daily_start))

    daily_df = process_daily(daily_src)
    daily_df.show(5)

    upsert_to_db(spark, daily_df, "daily_stats", key_cols=["symbol", "day_timestamp"])
    write_to_hdfs(daily_df, f"{HDFS_BASE_PATH}/batch/daily")

    set_last_processed(spark, "daily", max_ts)

    spark.stop()


if __name__ == "__main__":
    main()