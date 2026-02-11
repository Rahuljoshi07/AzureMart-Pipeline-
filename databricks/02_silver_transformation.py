# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Cleaning & Enrichment
# MAGIC Takes the raw Bronze data, deduplicates it, fixes formatting issues,
# MAGIC quarantines garbage rows, and adds some useful calculated fields.

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col, lit, trim, upper, lower, initcap, when, coalesce,
    current_timestamp, to_timestamp, to_date, date_format,
    row_number, count, sum as spark_sum, avg, round as spark_round,
    regexp_replace, sha2, concat_ws, monotonically_increasing_id,
    lag, lead, dense_rank, percent_rank
)
from pyspark.sql.types import DoubleType, IntegerType, DateType
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

STORAGE_ACCOUNT = "retaildatalake"
BRONZE_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/sales"
SILVER_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/sales"
QUARANTINE_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/_quarantine/sales"
METRICS_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/_metrics"

# Quality thresholds — tweak these if needed
MAX_NULL_PCT = 0.05
VALID_SEGMENTS = ["Premium", "Regular", "Budget"]
VALID_STATUSES = ["Completed", "Returned", "Cancelled"]
VALID_PAYMENT_METHODS = ["Credit Card", "Debit Card", "Cash", "Digital Wallet", "Bank Transfer"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Profiling

# COMMAND ----------

def profile_data(df, layer="input"):
    """Run a quick data quality scan — nulls, dupes, row count."""
    total = df.count()
    metrics = {"layer": layer, "total_rows": total, "timestamp": datetime.now().isoformat()}

    # Null analysis per column
    null_counts = {}
    for c in df.columns:
        if not c.startswith("_"):
            nulls = df.filter(col(c).isNull() | (col(c) == "")).count()
            null_counts[c] = {"count": nulls, "pct": round(nulls / total, 4) if total > 0 else 0}

    metrics["null_analysis"] = null_counts

    # Duplicate analysis
    dupes = total - df.dropDuplicates(["transaction_id"]).count()
    metrics["duplicate_count"] = dupes
    metrics["duplicate_pct"] = round(dupes / total, 4) if total > 0 else 0

    return metrics


def print_quality_report(metrics):
    """Dump quality metrics to stdout so we can see them in the notebook."""
    print(f"\n--- DATA QUALITY: {metrics['layer'].upper()} ---")
    print(f"  Total Rows : {metrics['total_rows']}")
    print(f"  Duplicates : {metrics['duplicate_count']} ({metrics['duplicate_pct']*100:.1f}%)")
    print(f"  Null columns (worst offenders):")

    sorted_nulls = sorted(
        metrics["null_analysis"].items(),
        key=lambda x: x[1]["count"], reverse=True
    )
    for col_name, info in sorted_nulls[:8]:
        if info["count"] > 0:
            print(f"    {col_name:<20}: {info['count']:<6} ({info['pct']*100:.1f}%)")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning Steps

# COMMAND ----------

def remove_duplicates(df):
    """Kill duplicate transactions — keep whichever was ingested last."""
    window = Window.partitionBy("transaction_id").orderBy(col("_ingestion_timestamp").desc())

    return (
        df
        .withColumn("_dup_rank", row_number().over(window))
        .filter(col("_dup_rank") == 1)
        .drop("_dup_rank")
    )


def clean_null_values(df):
    """Fill in missing values with sensible defaults so downstream doesn't break."""
    return (
        df
        # Swap empty strings for actual nulls first
        .withColumn("customer_name",
            when(trim(col("customer_name")) == "", lit(None))
            .otherwise(trim(col("customer_name"))))
        .withColumn("customer_email",
            when(trim(col("customer_email")) == "", lit(None))
            .otherwise(trim(col("customer_email"))))

        # Then backfill with defaults where needed
        .withColumn("customer_name",
            coalesce(col("customer_name"), lit("Unknown Customer")))
        .withColumn("customer_email",
            coalesce(col("customer_email"), lit("unknown@placeholder.com")))
        .withColumn("customer_segment",
            when(col("customer_segment").isin(VALID_SEGMENTS), col("customer_segment"))
            .otherwise(lit("Regular")))
        .withColumn("order_status",
            when(col("order_status").isin(VALID_STATUSES), col("order_status"))
            .otherwise(lit("Completed")))
        .withColumn("payment_method",
            when(col("payment_method").isin(VALID_PAYMENT_METHODS), col("payment_method"))
            .otherwise(lit("Unknown")))
    )


def standardize_formats(df):
    """Make everything consistent — proper casing, trimmed whitespace, correct types."""
    return (
        df
        # Text cleanup
        .withColumn("customer_name", initcap(trim(col("customer_name"))))
        .withColumn("product_name", trim(col("product_name")))
        .withColumn("category", initcap(trim(col("category"))))
        .withColumn("sub_category", initcap(trim(col("sub_category"))))
        .withColumn("brand", initcap(trim(col("brand"))))
        .withColumn("store_name", trim(col("store_name")))
        .withColumn("store_city", initcap(trim(col("store_city"))))
        .withColumn("store_state", upper(trim(col("store_state"))))

        # IDs should be uppercase and trimmed
        .withColumn("transaction_id", upper(trim(col("transaction_id"))))
        .withColumn("customer_id", upper(trim(col("customer_id"))))
        .withColumn("product_id", upper(trim(col("product_id"))))
        .withColumn("store_id", upper(trim(col("store_id"))))

        # Dates and numbers
        .withColumn("transaction_date", to_timestamp(col("transaction_date"), "yyyy-MM-dd HH:mm:ss"))

        # Cast numerics (just in case they came in as strings)
        .withColumn("unit_price", col("unit_price").cast(DoubleType()))
        .withColumn("quantity", col("quantity").cast(IntegerType()))
        .withColumn("discount_pct", col("discount_pct").cast(DoubleType()))
    )


def add_calculated_columns(df):
    """Add the derived fields the business actually cares about."""
    return (
        df
        .withColumn("total_amount",
            spark_round(col("unit_price") * col("quantity"), 2))
        .withColumn("discount_amount",
            spark_round(col("total_amount") * col("discount_pct"), 2))
        .withColumn("net_amount",
            spark_round(col("total_amount") - (col("total_amount") * col("discount_pct")), 2))
        .withColumn("transaction_date_key",
            date_format(col("transaction_date"), "yyyyMMdd").cast(IntegerType()))
        .withColumn("processing_timestamp", current_timestamp())
    )


def quarantine_bad_records(df):
    """
    Split the data into good and bad. Bad rows get a reason tag and
    go to quarantine for someone to look at later.
    """
    # These are our hard rules — if any of these fail, the record is quarantined
    valid_condition = (
        col("transaction_id").isNotNull() &
        col("transaction_date").isNotNull() &
        (col("unit_price") > 0) &
        (col("quantity") > 0) &
        (col("unit_price") < 50000) &
        (col("quantity") < 10000)
    )

    valid_df = df.filter(valid_condition)
    quarantine_df = df.filter(~valid_condition).withColumn(
        "_quarantine_reason",
        when(col("transaction_id").isNull(), "NULL_TRANSACTION_ID")
        .when(col("transaction_date").isNull(), "NULL_TRANSACTION_DATE")
        .when(col("unit_price") <= 0, "INVALID_PRICE")
        .when(col("quantity") <= 0, "INVALID_QUANTITY")
        .when(col("unit_price") >= 50000, "PRICE_EXCEEDS_MAX")
        .when(col("quantity") >= 10000, "QUANTITY_EXCEEDS_MAX")
        .otherwise("UNKNOWN")
    )

    return valid_df, quarantine_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Window Analytics
# MAGIC These add some useful columns that help with reporting further down the line.

# COMMAND ----------

def add_window_analytics(df):
    """
    Layer on some analytics columns using window functions.
    Stuff like purchase sequence per customer, prev/next amounts, category rankings, etc.
    """
    # Per-customer, chronological
    customer_window = Window.partitionBy("customer_id").orderBy("transaction_date")

    # Per-category, ranked by revenue (highest first)
    category_window = Window.partitionBy("category").orderBy(col("net_amount").desc())

    # Per-store running total
    store_window = (
        Window.partitionBy("store_id")
        .orderBy("transaction_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    return (
        df
        # Which purchase number is this for the customer?
        .withColumn("customer_purchase_seq",
            row_number().over(customer_window))

        # What did they spend last time? (lag)
        .withColumn("prev_purchase_amount",
            lag("net_amount", 1).over(customer_window))

        # What will they spend next? (lead)
        .withColumn("next_purchase_amount",
            lead("net_amount", 1).over(customer_window))

        # Where does this sale rank within its category?
        .withColumn("category_revenue_rank",
            dense_rank().over(category_window))

        # Cumulative revenue per store over time
        .withColumn("store_running_revenue",
            spark_sum("net_amount").over(store_window))

        # Percentile within category
        .withColumn("category_pct_rank",
            spark_round(percent_rank().over(category_window), 4))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental Load

# COMMAND ----------

def get_silver_watermark(spark):
    """Find the latest timestamp in Silver so we only process new data."""
    try:
        result = (
            spark.read.format("delta").load(SILVER_PATH)
            .selectExpr("MAX(processing_timestamp) as wm")
            .collect()[0]["wm"]
        )
        return result.isoformat() if result else "1900-01-01T00:00:00"
    except Exception:
        return "1900-01-01T00:00:00"


def get_incremental_bronze(spark, watermark):
    """Pull only the rows from Bronze that arrived after our watermark."""
    return (
        spark.read.format("delta").load(BRONZE_PATH)
        .filter(col("_ingestion_timestamp") > lit(watermark))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run It

# COMMAND ----------

def run_silver_transformation(spark, mode="incremental"):
    """Main Silver pipeline. Use mode='full' to reprocess everything or 'incremental' for just deltas."""
    print(f"[Silver] Starting {mode} transformation...")

    # Step 1: Get the right data
    if mode == "incremental":
        watermark = get_silver_watermark(spark)
        print(f"[Silver] Watermark: {watermark}")
        bronze_df = get_incremental_bronze(spark, watermark)
    else:
        bronze_df = spark.read.format("delta").load(BRONZE_PATH)

    if bronze_df.rdd.isEmpty():
        print("[Silver] No new data to process.")
        return

    # Step 2: See what we're working with
    input_metrics = profile_data(bronze_df, "bronze_input")
    print_quality_report(input_metrics)

    # Step 3: Run the cleaning pipeline
    cleaned_df = (
        bronze_df
        .transform(remove_duplicates)
        .transform(clean_null_values)
        .transform(standardize_formats)
        .transform(add_calculated_columns)
    )

    # Step 4: Pull out the bad rows
    valid_df, quarantine_df = quarantine_bad_records(cleaned_df)

    quarantine_count = quarantine_df.count()
    if quarantine_count > 0:
        print(f"[Silver] Heads up: {quarantine_count} records quarantined")
        (
            quarantine_df.write
            .format("delta")
            .mode("append")
            .partitionBy("_ingestion_date")
            .save(QUARANTINE_PATH)
        )

    # Step 5: Add the analytics columns
    enriched_df = add_window_analytics(valid_df)

    # Step 6: Check the output quality
    output_metrics = profile_data(enriched_df, "silver_output")
    print_quality_report(output_metrics)

    # Step 7: Pick just the columns we want (ditch internal metadata)
    silver_columns = [
        "transaction_id", "transaction_date", "transaction_date_key",
        "customer_id", "customer_name", "customer_email", "customer_segment",
        "product_id", "product_name", "category", "sub_category", "brand",
        "unit_price", "quantity", "discount_pct", "total_amount",
        "discount_amount", "net_amount",
        "store_id", "store_name", "store_city", "store_state",
        "payment_method", "order_status",
        "customer_purchase_seq", "prev_purchase_amount",
        "next_purchase_amount", "category_revenue_rank",
        "store_running_revenue", "category_pct_rank",
        "processing_timestamp", "_ingestion_date"
    ]
    final_df = enriched_df.select(*[col(c) for c in silver_columns if c in enriched_df.columns])

    # Step 8: Write it out
    (
        final_df.write
        .format("delta")
        .mode("append" if mode == "incremental" else "overwrite")
        .partitionBy("_ingestion_date")
        .option("mergeSchema", "true")
        .save(SILVER_PATH)
    )

    row_count = final_df.count()
    print(f"[Silver] Done — {row_count} records written ({mode} mode)")
    return {"rows_written": row_count, "quarantined": quarantine_count}


# Uncomment to run:
# run_silver_transformation(spark, mode="full")
# run_silver_transformation(spark, mode="incremental")
