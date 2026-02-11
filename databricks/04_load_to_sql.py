# Databricks notebook source
# MAGIC %md
# MAGIC # Load Gold Tables to Azure SQL
# MAGIC Reads the curated Delta tables from the Gold layer and pushes them
# MAGIC into Azure SQL so Power BI / reporting tools can query them.

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp
from datetime import datetime
import os

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

STORAGE_ACCOUNT = "retaildatalake"
GOLD_BASE = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# SQL connection string — in prod you'd pull creds from Key Vault
JDBC_URL = "jdbc:sqlserver://retail-sql-server.database.windows.net:1433;database=RetailDW"


def _get_secret(scope, key):
    """Try Databricks secrets first, fall back to env vars when running locally."""
    try:
        return dbutils.secrets.get(scope=scope, key=key)  # type: ignore[name-defined]
    except NameError:
        # Running outside Databricks — read from environment variables
        env_key = key.upper().replace("-", "_")
        return os.environ.get(env_key, f"<SET_{env_key}_ENV_VAR>")


SQL_PROPERTIES = {
    "user": _get_secret("retail-kv", "sql-username"),
    "password": _get_secret("retail-kv", "sql-password"),
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "encrypt": "true",
    "trustServerCertificate": "false",
    "hostNameInCertificate": "*.database.windows.net",
    "loginTimeout": "30",
}

# Table mappings: Gold Delta path → SQL table name
TABLE_MAP = {
    "dim_customer": {"gold_path": f"{GOLD_BASE}/dim_customer", "sql_table": "dbo.dim_customer"},
    "dim_product": {"gold_path": f"{GOLD_BASE}/dim_product", "sql_table": "dbo.dim_product"},
    "dim_store": {"gold_path": f"{GOLD_BASE}/dim_store", "sql_table": "dbo.dim_store"},
    "dim_date": {"gold_path": f"{GOLD_BASE}/dim_date", "sql_table": "dbo.dim_date"},
    "fact_sales": {"gold_path": f"{GOLD_BASE}/fact_sales", "sql_table": "dbo.fact_sales"},
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Functions

# COMMAND ----------

def load_to_sql(spark, df, sql_table, mode="overwrite", batch_size=50000):
    """Write a DataFrame to Azure SQL via JDBC. Pretty standard stuff."""
    print(f"  Loading {df.count()} rows → {sql_table} ({mode})...")

    (
        df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", sql_table)
        .option("batchsize", batch_size)
        .option("truncate", "true" if mode == "overwrite" else "false")
        .options(**SQL_PROPERTIES)
        .mode(mode)
        .save()
    )

    print(f"  Done: {sql_table}")


def load_dimension(spark, table_name, mode="overwrite"):
    """Load one dimension table from Gold Delta to SQL."""
    config = TABLE_MAP[table_name]
    df = spark.read.format("delta").load(config["gold_path"])

    # Make sure date columns play nice with SQL Server
    load_to_sql(spark, df, config["sql_table"], mode)
    return df.count()


def load_fact_sales(spark, mode="overwrite", partition_filter=None):
    """Load fact_sales. Optionally filter by date range for incremental loads."""
    config = TABLE_MAP["fact_sales"]
    df = spark.read.format("delta").load(config["gold_path"])

    if partition_filter:
        df = df.filter(partition_filter)
        print(f"  Applied filter: {partition_filter}")

    load_to_sql(spark, df, config["sql_table"], mode)
    return df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

def validate_sql_load(spark, sql_table):
    """Read back from SQL to make sure the data actually landed."""
    sql_df = (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", sql_table)
        .options(**SQL_PROPERTIES)
        .load()
    )

    row_count = sql_df.count()
    col_count = len(sql_df.columns)

    return {
        "table": sql_table,
        "row_count": row_count,
        "column_count": col_count,
        "columns": sql_df.columns,
    }


def run_integrity_checks(spark):
    """Check that fact table foreign keys actually point to real dimension rows."""
    checks = []

    # Check: All customer_sk in fact_sales exist in dim_customer
    fact_df = spark.read.format("jdbc").option("url", JDBC_URL) \
        .option("dbtable", "dbo.fact_sales").options(**SQL_PROPERTIES).load()
    dim_cust = spark.read.format("jdbc").option("url", JDBC_URL) \
        .option("dbtable", "dbo.dim_customer").options(**SQL_PROPERTIES).load()

    orphan_customers = fact_df.join(
        dim_cust, fact_df.customer_sk == dim_cust.customer_sk, "left_anti"
    ).filter(col("customer_sk") != -1).count()

    checks.append({
        "check": "fact_sales → dim_customer referential integrity",
        "orphan_count": orphan_customers,
        "status": "PASS" if orphan_customers == 0 else "WARN"
    })

    return checks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Full Load

# COMMAND ----------

def run_full_sql_load(spark):
    """Load all Gold tables into SQL. Dims first, then the fact table."""
    print("\n--- GOLD -> AZURE SQL LOAD ---")

    start_time = datetime.now()
    results = {}

    # Load dimensions first
    for dim in ["dim_date", "dim_customer", "dim_product", "dim_store"]:
        try:
            count = load_dimension(spark, dim)
            results[dim] = {"status": "success", "rows": count}
        except Exception as e:
            results[dim] = {"status": "failed", "error": str(e)}
            print(f"  FAILED: {dim} — {str(e)[:80]}")

    # Load fact table
    try:
        count = load_fact_sales(spark)
        results["fact_sales"] = {"status": "success", "rows": count}
    except Exception as e:
        results["fact_sales"] = {"status": "failed", "error": str(e)}
        print(f"  FAILED: fact_sales — {str(e)[:80]}")

    # Quick validation pass
    print("\nValidating...")
    for table_name, config in TABLE_MAP.items():
        if results.get(table_name, {}).get("status") == "success":
            val = validate_sql_load(spark, config["sql_table"])
            print(f"  {config['sql_table']}: {val['row_count']} rows, {val['column_count']} cols")

    duration = (datetime.now() - start_time).total_seconds()

    print(f"\n--- LOAD SUMMARY ---")
    for table, result in results.items():
        status = result['status'].upper()
        rows = result.get('rows', 'N/A')
        print(f"  {table:<18}: {status:<8} ({rows} rows)")
    print(f"  Duration: {duration:.1f}s\n")

    return results


# Uncomment to run:
# run_full_sql_load(spark)
