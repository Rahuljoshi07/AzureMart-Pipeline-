"""
Helper Utilities for Retail Data Lakehouse Pipeline
Common functions used across Bronze, Silver, and Gold layers.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit, count, sum as spark_sum
from pyspark.sql.types import StructType
from datetime import datetime, timedelta


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    """Safe Delta read with error handling."""
    try:
        return spark.read.format("delta").load(path)
    except Exception as e:
        print(f"[WARN] Cannot read Delta at {path}: {e}")
        return spark.createDataFrame([], StructType([]))


def write_delta(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    partition_by: list = None,
    coalesce_num: int = None,
):
    """Standardized Delta write with optional partitioning and coalescing."""
    writer = df
    if coalesce_num:
        writer = writer.coalesce(coalesce_num)

    w = writer.write.format("delta").mode(mode).option("mergeSchema", "true")
    if partition_by:
        w = w.partitionBy(*partition_by)
    w.save(path)


def get_table_stats(df: DataFrame, table_name: str = "") -> dict:
    """Quick statistics for a DataFrame."""
    row_count = df.count()
    col_count = len(df.columns)
    null_counts = {
        c: df.filter(col(c).isNull()).count()
        for c in df.columns[:10]  # Top 10 columns only
    }
    return {
        "table": table_name,
        "rows": row_count,
        "columns": col_count,
        "nulls": null_counts,
    }


def validate_schema(df: DataFrame, expected_columns: list) -> dict:
    """Validate DataFrame has expected columns."""
    actual = set(df.columns)
    expected = set(expected_columns)

    missing = expected - actual
    extra = actual - expected

    return {
        "valid": len(missing) == 0,
        "missing_columns": list(missing),
        "extra_columns": list(extra),
    }


def build_date_range(start: str, end: str) -> list:
    """Generate a list of date strings between start and end."""
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    return [(s + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((e - s).days + 1)]


def calculate_completeness(df: DataFrame) -> DataFrame:
    """
    Calculate completeness percentage for each column.
    Returns a DataFrame with column_name, total, non_null, completeness_pct.
    """
    total = df.count()
    if total == 0:
        return df

    stats = []
    for c in df.columns:
        non_null = df.filter(col(c).isNotNull()).count()
        stats.append({
            "column_name": c,
            "total": total,
            "non_null": non_null,
            "completeness_pct": round(non_null / total * 100, 2),
        })

    spark = df.sparkSession
    return spark.createDataFrame(stats)


def print_banner(title: str, width: int = 50):
    """Print a formatted banner for console output."""
    print(f"\n{'='*width}")
    print(f"  {title}")
    print(f"{'='*width}\n")
