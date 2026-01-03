# spark-apps/batch/batch_processor.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, min, max, count, stddev,
    date_trunc, current_timestamp, to_timestamp, lit, to_date,
    unix_timestamp, first, last
)
from pyspark.sql.window import Window
from datetime import timedelta
from typing import List, Optional
from datetime import datetime

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
    sql = f"""
    INSERT INTO batch_job_state(job_name, last_processed_at)
    VALUES ('{job_name}', '{ts.isoformat()}')
    ON CONFLICT (job_name) DO UPDATE
    SET last_processed_at = EXCLUDED.last_processed_at;
    """
    execute_sql(spark, sql)


def _read_realtime_prices_incremental(
    spark,
    start_ts: Optional[datetime],
    # you can also pass end_ts if you ever want bounded reads
) :
    """
    Read realtime_prices with JDBC predicate pushdown (reads less than full table).
    If start_ts is None => reads full table (first run).
    """
    print(">>> Reading data from Realtime Prices...")

    if start_ts is None:
        df = spark.read.jdbc(
            url=POSTGRES_JDBC_URL,
            table="realtime_prices",
            properties=POSTGRES_PROPERTIES
        )
        return df.withColumn("timestamp", to_timestamp(col("timestamp")))

    start_str = start_ts.strftime("%Y-%m-%d %H:%M:%S")

    # predicates: Spark will create one JDBC partition per predicate.
    # With small data, one predicate is fine and still gives pushdown filtering.
    predicates = [f"timestamp >= '{start_str}'"]

    df = spark.read.jdbc(
        url=POSTGRES_JDBC_URL,
        table="realtime_prices",
        properties=POSTGRES_PROPERTIES,
        predicates=predicates
    )
    return df.withColumn("timestamp", to_timestamp(col("timestamp")))


# ---------- Processors ----------
def process_hourly(df):
    """
    Hourly candle:
      - open_price  = first(price) by timestamp within hour
      - close_price = last(price)  by timestamp within hour
      - low/high/avg = min/max/avg price within hour
    Volume:
      - total_volume uses max(volume_24h) (rolling 24h snapshot) to avoid meaningless sum explosion
      - avg_volume stays as avg(volume_24h)
    """
    print(">>> Processing Hourly Stats...")

    base = df.withColumn("hour_timestamp", date_trunc("hour", col("timestamp"))) \
             .withColumn("__ts_sec", unix_timestamp(col("timestamp")))

    w = Window.partitionBy("symbol", "hour_timestamp").orderBy(col("__ts_sec"))

    with_oc = base.withColumn("__open", first(col("price"), ignorenulls=True).over(w)) \
                  .withColumn("__close", last(col("price"), ignorenulls=True).over(w))

    hourly = with_oc.groupBy("symbol", "hour_timestamp").agg(
        min("price").alias("low_price"),
        max("price").alias("high_price"),
        avg("price").alias("avg_price"),
        max("__open").alias("open_price"),
        max("__close").alias("close_price"),
        # volume_24h is rolling snapshot -> use max as representative for the hour
        max("volume_24h").alias("total_volume"),
        avg("volume_24h").alias("avg_volume"),
        avg("market_cap").alias("avg_market_cap"),
        stddev("price").alias("price_volatility"),
        count("*").alias("record_count"),
    ).withColumn("created_at", current_timestamp())

    return hourly


def process_daily(df):
    """
    Daily candle:
      - day key uses DATE (day_timestamp) to avoid timezone drift in uniqueness
      - open/close computed by first/last price ordered by timestamp within day
    Volume:
      - total_volume uses max(volume_24h) (rolling snapshot)
      - peak_volume stays max(volume_24h)
    """
    print(">>> Processing Daily Stats...")

    base = df.withColumn("day_timestamp", to_date(date_trunc("day", col("timestamp")))) \
             .withColumn("__ts_sec", unix_timestamp(col("timestamp")))

    w = Window.partitionBy("symbol", "day_timestamp").orderBy(col("__ts_sec"))

    with_oc = base.withColumn("__open", first(col("price"), ignorenulls=True).over(w)) \
                  .withColumn("__close", last(col("price"), ignorenulls=True).over(w))

    daily = with_oc.groupBy("symbol", "day_timestamp").agg(
        min("price").alias("low_price"),
        max("price").alias("high_price"),
        avg("price").alias("avg_price"),
        max("__open").alias("open_price"),
        max("__close").alias("close_price"),
        # rolling 24h snapshot -> use max to represent the day, not sum
        max("volume_24h").alias("total_volume"),
        max("volume_24h").alias("peak_volume"),
        stddev("price").alias("volatility"),
        count("*").alias("transaction_count"),
    ).withColumn(
        "price_change_percent",
        ((col("close_price") - col("open_price")) / col("open_price") * 100)
    ).withColumn("created_at", current_timestamp())

    return daily


# ---------- UPSERT via staging ----------
def upsert_to_db(spark, df, target_table: str, key_cols: List[str]):
    """
    1) write df -> staging (overwrite)
    2) INSERT ... SELECT ... ON CONFLICT (keys) DO UPDATE ...
    3) drop staging
    """
    print(f">>> UPSERT to DB Table: {target_table}...")
    staging = f"{target_table}__staging"

    df.write.jdbc(
        url=POSTGRES_JDBC_URL,
        table=staging,
        mode="overwrite",
        properties=POSTGRES_PROPERTIES
    )

    cols = df.columns
    q_cols = ", ".join([f'"{c}"' for c in cols])
    q_keys = ", ".join([f'"{k}"' for k in key_cols])

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

    # ===== Determine incremental starts with lookback =====
    last_hourly = get_last_processed(spark, "hourly")
    hourly_start = (last_hourly - timedelta(hours=2)) if last_hourly else None

    last_daily = get_last_processed(spark, "daily")
    daily_start = (last_daily - timedelta(days=2)) if last_daily else None

    # Read only what we need:
    # - Hourly needs >= hourly_start
    # - Daily needs >= daily_start
    # We'll read from the earliest of the two to reuse the same DF for both computations.
    if hourly_start is None and daily_start is None:
        global_start = None
    elif hourly_start is None:
        global_start = daily_start
    elif daily_start is None:
        global_start = hourly_start
    else:
        import builtins
        global_start = builtins.min(hourly_start, daily_start)

    raw_df = _read_realtime_prices_incremental(spark, global_start)

    # Quick emptiness check without full count scan
    if len(raw_df.select("symbol").take(1)) == 0:
        print("No data found!")
        spark.stop()
        return

    # max timestamp in the read window (progress marker)
    max_ts = raw_df.agg(max(col("timestamp")).alias("mx")).collect()[0]["mx"]
    if max_ts is None:
        print("No valid timestamps in data!")
        spark.stop()
        return

    # ===== Hourly =====
    hourly_src = raw_df if hourly_start is None else raw_df.filter(col("timestamp") >= lit(hourly_start))
    hourly_df = process_hourly(hourly_src)
    hourly_df.show(5)

    upsert_to_db(spark, hourly_df, "hourly_stats", key_cols=["symbol", "hour_timestamp"])
    write_to_hdfs(hourly_df, f"{HDFS_BASE_PATH}/batch/hourly")

    set_last_processed(spark, "hourly", max_ts)

    # ===== Daily =====
    daily_src = raw_df if daily_start is None else raw_df.filter(col("timestamp") >= lit(daily_start))
    daily_df = process_daily(daily_src)
    daily_df.show(5)

    upsert_to_db(spark, daily_df, "daily_stats", key_cols=["symbol", "day_timestamp"])
    write_to_hdfs(daily_df, f"{HDFS_BASE_PATH}/batch/daily")

    set_last_processed(spark, "daily", max_ts)

    spark.stop()


if __name__ == "__main__":
    main()