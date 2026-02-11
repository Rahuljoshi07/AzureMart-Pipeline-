# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Star Schema
# MAGIC This is where everything comes together. We build proper dimension tables
# MAGIC (with SCD Type 2 for customers and products) and a fact table that
# MAGIC the BI tools can hit directly.

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col, lit, current_timestamp, monotonically_increasing_id,
    row_number, dense_rank, when, coalesce, concat, lpad,
    to_date, date_format, year, quarter, month, dayofmonth,
    dayofweek, weekofyear, expr, max as spark_max,
    min as spark_min, sum as spark_sum, count as spark_count,
    avg as spark_avg, round as spark_round, sha2, concat_ws,
    first, collect_set, array_contains
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType, TimestampType, BooleanType, LongType
)
from datetime import datetime, date, timedelta
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

STORAGE_ACCOUNT = "retaildatalake"
SILVER_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/sales"
GOLD_BASE = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"

GOLD_PATHS = {
    "dim_customer": f"{GOLD_BASE}/dim_customer",
    "dim_product": f"{GOLD_BASE}/dim_product",
    "dim_store": f"{GOLD_BASE}/dim_store",
    "dim_date": f"{GOLD_BASE}/dim_date",
    "fact_sales": f"{GOLD_BASE}/fact_sales",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Surrogate Keys

# COMMAND ----------

def generate_surrogate_keys(df, key_column, prefix=""):
    """Create integer surrogate keys via dense_rank. More stable than monotonically_increasing_id."""
    window = Window.orderBy(key_column)
    sk_col = f"{prefix}_sk" if prefix else f"{key_column}_sk"

    return df.withColumn(sk_col, dense_rank().over(window))


def generate_hash_key(df, columns, key_name):
    """SHA-256 hash key from multiple columns. Useful for composite business keys."""
    return df.withColumn(
        key_name,
        sha2(concat_ws("||", *[col(c).cast(StringType()) for c in columns]), 256)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: dim_date

# COMMAND ----------

def build_dim_date(spark, start_date="2024-01-01", end_date="2026-12-31"):
    """Build a calendar dimension. Covers 3 years so we have plenty of room."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    num_days = (end - start).days + 1

    # Create one row per day
    date_df = spark.range(0, num_days).selectExpr(
        f"date_add('{start_date}', cast(id as int)) as full_date"
    )

    # Simplified US holiday list (add more as needed)
    holidays = [
        "2025-01-01", "2025-01-20", "2025-02-17", "2025-05-26",
        "2025-07-04", "2025-09-01", "2025-10-13", "2025-11-11",
        "2025-11-27", "2025-12-25",
        "2026-01-01", "2026-01-19", "2026-02-16", "2026-05-25",
        "2026-07-04", "2026-09-07", "2026-10-12", "2026-11-11",
        "2026-11-26", "2026-12-25",
    ]

    dim_date = (
        date_df
        .withColumn("date_key", date_format(col("full_date"), "yyyyMMdd").cast(IntegerType()))
        .withColumn("year", year(col("full_date")))
        .withColumn("quarter", quarter(col("full_date")))
        .withColumn("month", month(col("full_date")))
        .withColumn("month_name", date_format(col("full_date"), "MMMM"))
        .withColumn("week_of_year", weekofyear(col("full_date")))
        .withColumn("day_of_month", dayofmonth(col("full_date")))
        .withColumn("day_of_week", dayofweek(col("full_date")))
        .withColumn("day_name", date_format(col("full_date"), "EEEE"))
        .withColumn("is_weekend",
            when(dayofweek(col("full_date")).isin(1, 7), lit(True)).otherwise(lit(False)))
        .withColumn("is_holiday",
            when(date_format(col("full_date"), "yyyy-MM-dd").isin(holidays), lit(True))
            .otherwise(lit(False)))
        .withColumn("fiscal_year",
            when(month(col("full_date")) >= 7, year(col("full_date")) + 1)
            .otherwise(year(col("full_date"))))
        .withColumn("fiscal_quarter",
            when(quarter(col("full_date")).isin(3, 4), quarter(col("full_date")) - 2)
            .otherwise(quarter(col("full_date")) + 2))
    )

    return dim_date

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_customer (SCD Type 2)

# COMMAND ----------

def build_dim_customer(spark, silver_df):
    """Extract distinct customers from Silver. Takes the latest record for each customer_id."""
    # Grab the most recent record per customer
    customer_window = Window.partitionBy("customer_id").orderBy(col("transaction_date").desc())

    dim_customer = (
        silver_df
        .withColumn("_rn", row_number().over(customer_window))
        .filter(col("_rn") == 1)
        .select(
            col("customer_id"),
            col("customer_name"),
            col("customer_email"),
            col("customer_segment"),
        )
        .distinct()
        .withColumn("customer_sk", dense_rank().over(Window.orderBy("customer_id")))
        .withColumn("effective_date", lit(date.today()))
        .withColumn("end_date", lit(date(9999, 12, 31)))
        .withColumn("is_current", lit(True))
        .select(
            "customer_sk", "customer_id", "customer_name",
            "customer_email", "customer_segment",
            "effective_date", "end_date", "is_current"
        )
    )

    return dim_customer


def merge_dim_customer_scd2(spark, new_customers_df, target_path):
    """
    SCD Type 2 merge using Delta Lake MERGE.
    If a customer's attributes changed, expire the old row and insert a new one.
    """
    try:
        target_table = DeltaTable.forPath(spark, target_path)

        # Identify changed records — expire old, insert new
        (
            target_table.alias("target")
            .merge(
                new_customers_df.alias("source"),
                "target.customer_id = source.customer_id AND target.is_current = true"
            )
            # When attributes changed, close out the old row
            .whenMatchedUpdate(
                condition="""
                    target.customer_name != source.customer_name OR
                    target.customer_email != source.customer_email OR
                    target.customer_segment != source.customer_segment
                """,
                set={
                    "is_current": lit(False),
                    "end_date": current_timestamp().cast(DateType())
                }
            )
            # Brand new customers — just insert them
            .whenNotMatchedInsertAll()
            .execute()
        )
        print("[Gold] dim_customer SCD2 merge done")

    except Exception:
        # First time — just write the table
        new_customers_df.write.format("delta").mode("overwrite").save(target_path)
        print("[Gold] dim_customer initial load done")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_product (SCD Type 2)

# COMMAND ----------

def build_dim_product(spark, silver_df):
    """Product dimension. Grabs the latest price for each product."""
    product_window = Window.partitionBy("product_id").orderBy(col("transaction_date").desc())

    dim_product = (
        silver_df
        .withColumn("_rn", row_number().over(product_window))
        .filter(col("_rn") == 1)
        .select(
            col("product_id"),
            col("product_name"),
            col("category"),
            col("sub_category"),
            col("brand"),
            col("unit_price").alias("current_price"),
        )
        .distinct()
        .withColumn("product_sk", dense_rank().over(Window.orderBy("product_id")))
        .withColumn("effective_date", lit(date.today()))
        .withColumn("end_date", lit(date(9999, 12, 31)))
        .withColumn("is_current", lit(True))
        .select(
            "product_sk", "product_id", "product_name",
            "category", "sub_category", "brand", "current_price",
            "effective_date", "end_date", "is_current"
        )
    )

    return dim_product

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_store

# COMMAND ----------

def build_dim_store(spark, silver_df):
    """Store dimension — pretty straightforward, one row per store."""
    store_window = Window.partitionBy("store_id").orderBy(col("transaction_date").desc())

    dim_store = (
        silver_df
        .withColumn("_rn", row_number().over(store_window))
        .filter(col("_rn") == 1)
        .select("store_id", "store_name", "store_city", "store_state")
        .distinct()
        .withColumn("store_sk", dense_rank().over(Window.orderBy("store_id")))
        .withColumn("effective_date", lit(date.today()))
        .withColumn("end_date", lit(date(9999, 12, 31)))
        .withColumn("is_current", lit(True))
        .select(
            "store_sk", "store_id", "store_name",
            "store_city", "store_state",
            "effective_date", "end_date", "is_current"
        )
    )

    return dim_store

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_sales

# COMMAND ----------

def build_fact_sales(spark, silver_df, dim_customer, dim_product, dim_store):
    """
    Join Silver data with dimension tables to get surrogate keys.
    Any unmatched dimension gets SK = -1 (the 'unknown member' row).
    """
    fact_sales = (
        silver_df
        # Look up customer SK
        .join(
            dim_customer.filter(col("is_current") == True)
                .select("customer_id", col("customer_sk")),
            on="customer_id",
            how="left"
        )
        # Look up product SK
        .join(
            dim_product.filter(col("is_current") == True)
                .select("product_id", col("product_sk")),
            on="product_id",
            how="left"
        )
        # Look up store SK
        .join(
            dim_store.filter(col("is_current") == True)
                .select("store_id", col("store_sk")),
            on="store_id",
            how="left"
        )
        # Generate a unique id for each fact row
        .withColumn("sales_sk", monotonically_increasing_id())
        # Date key for joining with dim_date
        .withColumn("date_key",
            date_format(col("transaction_date"), "yyyyMMdd").cast(IntegerType()))
        # Select fact columns
        .select(
            "sales_sk",
            "transaction_id",
            "date_key",
            col("customer_sk").cast(LongType()),
            col("product_sk").cast(LongType()),
            col("store_sk").cast(LongType()),
            "quantity",
            "unit_price",
            "discount_pct",
            "total_amount",
            col("net_amount"),
            "payment_method",
            "order_status",
            "processing_timestamp",
        )
    )

    # If a dimension lookup didn't match, stick in -1 instead of null
    for sk_col in ["customer_sk", "product_sk", "store_sk"]:
        fact_sales = fact_sales.withColumn(
            sk_col, coalesce(col(sk_col), lit(-1))
        )

    return fact_sales

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run It

# COMMAND ----------

def run_gold_layer(spark, mode="full"):
    """Build all dimensions and the fact table. Use mode='incremental' for SCD2 merges."""
    print(f"[Gold] Starting {mode} build...")
    start_time = datetime.now()

    # Read Silver
    silver_df = spark.read.format("delta").load(SILVER_PATH)
    silver_count = silver_df.count()
    print(f"[Gold] Silver records: {silver_count}")

    # dim_date is independent of the transaction data
    print("[Gold] Building dim_date...")
    dim_date = build_dim_date(spark)
    dim_date.write.format("delta").mode("overwrite").save(GOLD_PATHS["dim_date"])
    print(f"[Gold] dim_date: {dim_date.count()} rows")

    # Customer dimension
    print("[Gold] Building dim_customer...")
    dim_customer = build_dim_customer(spark, silver_df)
    if mode == "incremental":
        merge_dim_customer_scd2(spark, dim_customer, GOLD_PATHS["dim_customer"])
    else:
        dim_customer.write.format("delta").mode("overwrite").save(GOLD_PATHS["dim_customer"])
    print(f"[Gold] dim_customer: {dim_customer.count()} rows")

    # Product dimension
    print("[Gold] Building dim_product...")
    dim_product = build_dim_product(spark, silver_df)
    dim_product.write.format("delta").mode("overwrite").save(GOLD_PATHS["dim_product"])
    print(f"[Gold] dim_product: {dim_product.count()} rows")

    # Store dimension
    print("[Gold] Building dim_store...")
    dim_store = build_dim_store(spark, silver_df)
    dim_store.write.format("delta").mode("overwrite").save(GOLD_PATHS["dim_store"])
    print(f"[Gold] dim_store: {dim_store.count()} rows")

    # Fact table — needs all dimensions loaded first
    print("[Gold] Building fact_sales...")
    # Re-read dimensions from Delta so everything is consistent
    dim_customer_final = spark.read.format("delta").load(GOLD_PATHS["dim_customer"])
    dim_product_final = spark.read.format("delta").load(GOLD_PATHS["dim_product"])
    dim_store_final = spark.read.format("delta").load(GOLD_PATHS["dim_store"])

    fact_sales = build_fact_sales(spark, silver_df, dim_customer_final, dim_product_final, dim_store_final)
    fact_sales.write.format("delta").mode("overwrite").partitionBy("date_key").save(GOLD_PATHS["fact_sales"])
    fact_count = fact_sales.count()
    print(f"[Gold] fact_sales: {fact_count} rows")

    # Summary
    duration = (datetime.now() - start_time).total_seconds()
    print(f"\n--- GOLD BUILD COMPLETE ---")
    print(f"  Mode         : {mode}")
    print(f"  Silver input : {silver_count}")
    print(f"  dim_date     : {dim_date.count()}")
    print(f"  dim_customer : {dim_customer.count()}")
    print(f"  dim_product  : {dim_product.count()}")
    print(f"  dim_store    : {dim_store.count()}")
    print(f"  fact_sales   : {fact_count}")
    print(f"  Took         : {duration:.1f}s")

    return {
        "silver_input": silver_count,
        "fact_sales": fact_count,
        "duration_sec": duration,
    }


# Uncomment to run:
# run_gold_layer(spark, mode="full")
# run_gold_layer(spark, mode="incremental")
