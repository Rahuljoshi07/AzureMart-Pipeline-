# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw Data Ingestion
# MAGIC Reads raw CSV files from the data lake, tacks on some metadata columns,
# MAGIC and dumps everything into Delta as-is. No transformations here.

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, input_file_name, sha2, concat_ws
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)
from datetime import datetime, timedelta
import logging

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# Storage paths
STORAGE_ACCOUNT = "retaildatalake"
RAW_CONTAINER = "raw"
BRONZE_CONTAINER = "bronze"

RAW_BASE_PATH = f"abfss://{RAW_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/sales"
BRONZE_BASE_PATH = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/sales"
CHECKPOINT_PATH = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/_checkpoints/bronze_sales"

# We're explicit about the schema so Spark doesn't guess wrong on column types
RAW_SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("customer_email", StringType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("sub_category", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("discount_pct", DoubleType(), True),
    StructField("store_id", StringType(), True),
    StructField("store_name", StringType(), True),
    StructField("store_city", StringType(), True),
    StructField("store_state", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("order_status", StringType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Core Functions

# COMMAND ----------

def read_raw_csv(spark, path, schema):
    """Read CSVs with our schema. Permissive mode so we don't lose corrupted rows."""
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("multiLine", "false")
        .schema(schema)
        .load(path)
    )


def add_bronze_metadata(df):
    """Stamp each row with ingestion time, source file, and a hash for change detection."""
    return (
        df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_row_hash", sha2(
            concat_ws("||", *[col(c) for c in df.columns]), 256
        ))
        .withColumn("_ingestion_date", lit(datetime.now().strftime("%Y-%m-%d")))
    )


def write_bronze_delta(df, target_path, partition_col="_ingestion_date"):
    """Append to the Bronze Delta table. Partitioned by ingestion date so we can query efficiently."""
    (
        df.write
        .format("delta")
        .mode("append")
        .partitionBy(partition_col)
        .option("mergeSchema", "true")
        .save(target_path)
    )


def get_watermark(spark, bronze_path):
    """Grab the most recent ingestion date from Bronze. Defaults to 1900 if the table doesn't exist yet."""
    try:
        max_date = (
            spark.read.format("delta").load(bronze_path)
            .selectExpr("MAX(_ingestion_date) as max_date")
            .collect()[0]["max_date"]
        )
        return max_date if max_date else "1900-01-01"
    except Exception:
        return "1900-01-01"


def log_ingestion_stats(df, layer="bronze"):
    """Quick stats printout so we can eyeball the ingestion in the notebook output."""
    total_rows = df.count()
    null_txn_ids = df.filter(col("transaction_id").isNull()).count()
    distinct_dates = df.select("transaction_date").distinct().count()

    print(f"\n--- {layer.upper()} INGESTION STATS ---")
    print(f"  Rows ingested : {total_rows}")
    print(f"  Null txn IDs  : {null_txn_ids}")
    print(f"  Distinct dates: {distinct_dates}")
    print(f"  Ran at        : {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    return {"total_rows": total_rows, "null_ids": null_txn_ids, "dates": distinct_dates}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run It

# COMMAND ----------

def run_bronze_ingestion(spark, ingestion_date=None):
    """Main entry point. Pass a date string (YYYY-MM-DD) or leave blank for today."""
    if ingestion_date is None:
        ingestion_date = datetime.now().strftime("%Y-%m-%d")

    # Figure out which date folder to read
    date_parts = ingestion_date.split("-")
    date_path = f"{RAW_BASE_PATH}/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}/"

    print(f"[Bronze] Reading raw data from: {date_path}")

    try:
        # Read -> add metadata -> log -> write
        raw_df = read_raw_csv(spark, date_path, RAW_SCHEMA)

        bronze_df = add_bronze_metadata(raw_df)

        stats = log_ingestion_stats(bronze_df, "bronze")

        write_bronze_delta(bronze_df, BRONZE_BASE_PATH)

        print(f"[Bronze] Done — ingested {stats['total_rows']} records for {ingestion_date}")
        return stats

    except Exception as e:
        print(f"[Bronze] FAILED: {str(e)}")
        raise


# Uncomment to run:
# run_bronze_ingestion(spark, "2025-01-15")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Mode (backfill)

# COMMAND ----------

def run_bronze_batch(spark, start_date, end_date):
    """Process a range of dates. Good for the initial historical backfill."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    current = start
    results = []

    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        try:
            stats = run_bronze_ingestion(spark, date_str)
            results.append({"date": date_str, "status": "success", **stats})
        except Exception as e:
            results.append({"date": date_str, "status": "failed", "error": str(e)})
        current += timedelta(days=1)

    # Quick summary
    success = sum(1 for r in results if r["status"] == "success")
    failed = sum(1 for r in results if r["status"] == "failed")
    print(f"\n[Bronze Batch] Done: {success} ok, {failed} failed out of {len(results)} days")
    return results

# Uncomment to backfill:
# run_bronze_batch(spark, "2025-01-01", "2025-01-30")
