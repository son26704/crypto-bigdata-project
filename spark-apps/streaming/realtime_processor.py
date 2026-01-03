# spark-apps/streaming/realtime_processor.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, lit, concat, current_timestamp, from_json,
    max as sql_max, unix_timestamp, when, lag, abs as sql_abs
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)
from pyspark.sql.window import Window
from pathlib import Path
import importlib.util
import sys

def load_streaming_config_or_default():
    cfg_path = Path(__file__).with_name("config.py")
    if cfg_path.exists():
        spec = importlib.util.spec_from_file_location("streaming_config", str(cfg_path))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    class DefaultCfg:
        KAFKA_BOOTSTRAP_SERVERS = "kafka-0.kafka.crypto-bigdata.svc.cluster.local:9092"
        KAFKA_TOPIC = "crypto-prices"
        APP_NAME = "CryptoRealtimeProcessor"
        CHECKPOINT_LOCATION = "/tmp/checkpoint_realtime_v4"
        MICRO_BATCH_INTERVAL = "30 seconds"
        MA_WINDOWS = {"ma_5min": 5, "ma_15min": 15, "ma_1hour": 60}
        ALERT_THRESHOLDS = {"price_change_percent": 5.0, "volume_spike_multiplier": 3.0}
        ENABLE_MA_CROSSOVER_ALERT = True

    print("Streaming config not found at /tmp/spark-apps/streaming/config.py; using built-in defaults.")
    return DefaultCfg()

CFG = load_streaming_config_or_default()

KAFKA_BOOTSTRAP_SERVERS = CFG.KAFKA_BOOTSTRAP_SERVERS
KAFKA_TOPIC = CFG.KAFKA_TOPIC
CHECKPOINT_LOCATION = CFG.CHECKPOINT_LOCATION
APP_NAME = CFG.APP_NAME
MICRO_BATCH_INTERVAL = CFG.MICRO_BATCH_INTERVAL
MA_WINDOWS = CFG.MA_WINDOWS
ALERT_THRESHOLDS = CFG.ALERT_THRESHOLDS
ENABLE_MA_CROSSOVER_ALERT = getattr(CFG, "ENABLE_MA_CROSSOVER_ALERT", True)

# --- Postgres config (kept as your current behavior) ---
sys.path.append("/tmp/spark-apps/common")
sys.path.append("/opt/spark-apps/common")

POSTGRES_JDBC_URL = None
POSTGRES_PROPERTIES = None

try:
    # preferred: postgres_config.py
    from postgres_config import POSTGRES_JDBC_URL, POSTGRES_PROPERTIES
except Exception:
    try:
        # fallback: config.py (older style)
        from config import POSTGRES_JDBC_URL, POSTGRES_PROPERTIES
    except Exception:
        print("Could not load Postgres config file, using default hardcoded values")
        POSTGRES_JDBC_URL = "jdbc:postgresql://postgres.crypto-bigdata.svc.cluster.local:5432/cryptodb"
        POSTGRES_PROPERTIES = {
            "user": "cryptouser",
            "password": "cryptopass123",
            "driver": "org.postgresql.Driver"
        }

def create_spark_session():
    return SparkSession.builder \
        .appName(APP_NAME) \
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
        StructField("ingestion_timestamp", StringType(), True),
    ])

def is_df_empty(df) -> bool:
    return len(df.take(1)) == 0

def read_recent_history(spark, symbols, min_ts_iso, max_ts_iso):
    if not symbols:
        return None

    sym_list = ",".join(["'" + s.replace("'", "''") + "'" for s in symbols])
    predicate = (
        f"symbol IN ({sym_list}) "
        f"AND timestamp >= '{min_ts_iso}' "
        f"AND timestamp <= '{max_ts_iso}'"
    )

    hist = spark.read.jdbc(
        url=POSTGRES_JDBC_URL,
        table="realtime_prices",
        properties=POSTGRES_PROPERTIES,
        predicates=[predicate]
    )
    return hist

def process_batch(batch_df, batch_id):
    if is_df_empty(batch_df):
        return

    print(f"--- Processing Batch {batch_id} ---")

    spark = batch_df.sparkSession

    # 1) Normalize event time from ingestion_timestamp
    current_df = batch_df.withColumn("timestamp", col("ingestion_timestamp").cast("timestamp")) \
                         .withColumn("__is_current", lit(1)) \
                         .filter(col("timestamp").isNotNull())

    if is_df_empty(current_df):
        print("Batch has no valid timestamps after casting. Skipping.")
        return

    # 2) Determine time range we need (largest MA window)
    max_ts = current_df.agg(sql_max(col("timestamp")).alias("max_ts")).collect()[0]["max_ts"]
    max_ts_iso = max_ts.strftime("%Y-%m-%d %H:%M:%S")

    max_window_min = max(MA_WINDOWS.values())
    min_ts = spark.sql(
        f"SELECT timestamp('{max_ts_iso}') - INTERVAL {max_window_min + 2} MINUTES AS min_ts"
    ).collect()[0]["min_ts"]
    min_ts_iso = min_ts.strftime("%Y-%m-%d %H:%M:%S")

    symbols = [r["symbol"] for r in current_df.select("symbol").distinct().collect() if r["symbol"]]
    if not symbols:
        print("No symbols found in batch. Skipping.")
        return

    # 3) Read DB history and union with current batch (for true time-based rolling windows)
    hist = read_recent_history(spark, symbols, min_ts_iso, max_ts_iso)

    if hist is None or is_df_empty(hist):
        union_df = current_df.withColumn("__is_history", lit(0))
    else:
        # Prepare history rows to match columns needed
        hist_prepared = hist.select(
            lit(None).cast("string").alias("id"),
            col("symbol").cast("string").alias("symbol"),
            lit(None).cast("string").alias("name"),
            lit(None).cast("string").alias("image"),
            col("price").cast("double").alias("price"),
            lit(None).cast("double").alias("market_cap"),
            col("volume_24h").cast("double").alias("volume_24h"),
            lit(None).cast("double").alias("high_24h"),
            lit(None).cast("double").alias("low_24h"),
            lit(None).cast("double").alias("ath"),
            lit(None).cast("double").alias("atl"),
            lit(None).cast("double").alias("price_change_24h"),
            lit(None).cast("double").alias("price_change_percentage_24h"),
            lit(None).cast("bigint").alias("last_updated"),
            lit(None).cast("string").alias("ingestion_timestamp"),
            col("timestamp").cast("timestamp").alias("timestamp"),
            lit(0).alias("__is_current"),
        ).withColumn("__is_history", lit(1))

        union_df = current_df.withColumn("__is_history", lit(0)).unionByName(hist_prepared, allowMissingColumns=True)

    # 4) Rolling windows by time (seconds)
    union_df = union_df.withColumn("__ts_sec", unix_timestamp(col("timestamp")))
    w_base = Window.partitionBy("symbol").orderBy(col("__ts_sec"))

    def range_w(minutes: int):
        return w_base.rangeBetween(-minutes * 60, 0)

    union_df = union_df \
        .withColumn("ma_5min", avg("price").over(range_w(MA_WINDOWS["ma_5min"]))) \
        .withColumn("ma_15min", avg("price").over(range_w(MA_WINDOWS["ma_15min"]))) \
        .withColumn("ma_1hour", avg("price").over(range_w(MA_WINDOWS["ma_1hour"]))) \
        .withColumn("vol_avg_15min", avg("volume_24h").over(range_w(MA_WINDOWS["ma_15min"])))

    # 5) Keep only current rows for writing outputs
    processed_current = union_df.filter(col("__is_current") == 1) \
        .withColumn("created_at", current_timestamp())

    # 6) Alerts (edge-triggered to reduce spam)
    price_thr = float(ALERT_THRESHOLDS.get("price_change_percent", 5.0))
    vol_mult = float(ALERT_THRESHOLDS.get("volume_spike_multiplier", 3.0))

    # A) volatility by 24h % change: trigger only when condition turns False->True
    union_df = union_df.withColumn(
        "__volatility_cond",
        (col("price_change_percentage_24h").isNotNull()) & (sql_abs(col("price_change_percentage_24h")) > lit(price_thr))
    ).withColumn(
        "__prev_volatility_cond",
        lag(col("__volatility_cond"), 1).over(w_base)
    )

    volatility_alerts = union_df.filter(
        (col("__is_current") == 1) &
        (col("__volatility_cond") == True) &
        ((col("__prev_volatility_cond").isNull()) | (col("__prev_volatility_cond") == False))
    ).withColumn("alert_type", lit("VOLATILITY_WARNING")) \
     .withColumn("message", concat(lit("High 24h volatility detected for "), col("symbol"))) \
     .withColumn("created_at", current_timestamp()) \
     .select(
        "symbol", "alert_type", "message",
        col("price").alias("price_after"),
        col("price_change_percentage_24h").alias("change_percentage"),
        "timestamp", "created_at"
     )

    # B) volume spike: volume_24h > multiplier * rolling avg volume_15m (edge-triggered)
    union_df = union_df.withColumn(
        "__volume_spike_cond",
        (col("volume_24h").isNotNull()) &
        (col("vol_avg_15min").isNotNull()) &
        (col("vol_avg_15min") > lit(0.0)) &
        (col("volume_24h") > lit(vol_mult) * col("vol_avg_15min"))
    ).withColumn(
        "__prev_volume_spike_cond",
        lag(col("__volume_spike_cond"), 1).over(w_base)
    )

    volume_alerts = union_df.filter(
        (col("__is_current") == 1) &
        (col("__volume_spike_cond") == True) &
        ((col("__prev_volume_spike_cond").isNull()) | (col("__prev_volume_spike_cond") == False))
    ).withColumn("alert_type", lit("VOLUME_SPIKE")) \
     .withColumn("message", concat(lit("Volume spike detected for "), col("symbol"), lit(" (vs 15m avg)"))) \
     .withColumn("created_at", current_timestamp()) \
     .withColumn(
        "__ratio",
        when(col("vol_avg_15min") > 0, col("volume_24h") / col("vol_avg_15min")).otherwise(lit(None))
     ) \
     .select(
        "symbol", "alert_type", "message",
        col("price").alias("price_after"),
        col("__ratio").alias("change_percentage"),
        "timestamp", "created_at"
     )

    # C) MA crossover: price crosses MA(15m) (optional)
    ma_alerts = None
    if ENABLE_MA_CROSSOVER_ALERT:
        union_df = union_df.withColumn("__prev_price", lag(col("price"), 1).over(w_base)) \
                           .withColumn("__prev_ma15", lag(col("ma_15min"), 1).over(w_base))

        cross_up = (
            col("price").isNotNull() & col("ma_15min").isNotNull() &
            col("__prev_price").isNotNull() & col("__prev_ma15").isNotNull() &
            (col("__prev_price") <= col("__prev_ma15")) & (col("price") > col("ma_15min"))
        )
        cross_down = (
            col("price").isNotNull() & col("ma_15min").isNotNull() &
            col("__prev_price").isNotNull() & col("__prev_ma15").isNotNull() &
            (col("__prev_price") >= col("__prev_ma15")) & (col("price") < col("ma_15min"))
        )

        ma_alerts = union_df.filter((col("__is_current") == 1) & (cross_up | cross_down)) \
            .withColumn(
                "alert_type",
                when(cross_up, lit("MA_CROSSOVER_UP")).otherwise(lit("MA_CROSSOVER_DOWN"))
            ) \
            .withColumn(
                "message",
                when(cross_up, concat(lit("Price crossed ABOVE MA(15m) for "), col("symbol")))
                .otherwise(concat(lit("Price crossed BELOW MA(15m) for "), col("symbol")))
            ) \
            .withColumn("created_at", current_timestamp()) \
            .withColumn(
                "__pct_from_ma",
                when(col("ma_15min") > 0, (col("price") - col("ma_15min")) / col("ma_15min") * 100.0)
                .otherwise(lit(None))
            ) \
            .select(
                "symbol", "alert_type", "message",
                col("price").alias("price_after"),
                col("__pct_from_ma").alias("change_percentage"),
                "timestamp", "created_at"
            )

    # 7) Write realtime prices (same columns/table as before)
    try:
        write_df = processed_current.select(
            "symbol", "name", "image", "price", "market_cap", "volume_24h",
            "high_24h", "low_24h", "ath", "atl",
            "price_change_24h", "price_change_percentage_24h",
            "ma_5min", "ma_15min", "ma_1hour",
            "timestamp", "created_at"
        )
        write_df.write.jdbc(
            url=POSTGRES_JDBC_URL,
            table="realtime_prices",
            mode="append",
            properties=POSTGRES_PROPERTIES
        )
        print("✓ Wrote prices to DB")
    except Exception as e:
        print(f"✗ Error prices: {e}")

    # 8) Write alerts (union)
    all_alerts = volatility_alerts.unionByName(volume_alerts, allowMissingColumns=True)
    if ma_alerts is not None:
        all_alerts = all_alerts.unionByName(ma_alerts, allowMissingColumns=True)

    if not is_df_empty(all_alerts):
        try:
            all_alerts.write.jdbc(
                url=POSTGRES_JDBC_URL,
                table="alerts",
                mode="append",
                properties=POSTGRES_PROPERTIES
            )
            print("✓ Wrote alerts")
        except Exception as e:
            print(f"✗ Error alerts: {e}")

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
        .trigger(processingTime=MICRO_BATCH_INTERVAL) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()