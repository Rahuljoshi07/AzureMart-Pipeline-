# pipeline_config.py
# Keeps all the pipeline settings in one place so we don't have
# magic strings floating around everywhere.

from dataclasses import dataclass, field
from typing import Dict, List
from datetime import datetime


@dataclass
class StorageConfig:
    """ADLS Gen2 storage details. Update the account name for your env."""
    storage_account: str = "retaildatalake"
    container_raw: str = "raw"
    container_bronze: str = "bronze"
    container_silver: str = "silver"
    container_gold: str = "gold"

    @property
    def base_path(self) -> str:
        return f"abfss://{{container}}@{self.storage_account}.dfs.core.windows.net"

    def get_path(self, layer, table="", partition_date=""):
        """Build the full ABFSS path for a given layer + optional table/date."""
        container = getattr(self, f"container_{layer}", layer)
        path = f"abfss://{container}@{self.storage_account}.dfs.core.windows.net"
        if table:
            path += f"/{table}"
        if partition_date:
            path += f"/ingestion_date={partition_date}"
        return path


@dataclass
class SchemaConfig:
    """
    Expected columns at each layer.
    Handy for validation and quick reference when debugging.
    """
    raw_sales_columns: List[str] = field(default_factory=lambda: [
        "transaction_id", "transaction_date", "customer_id", "customer_name",
        "customer_email", "customer_segment", "product_id", "product_name",
        "category", "sub_category", "brand", "unit_price", "quantity",
        "discount_pct", "store_id", "store_name", "store_city",
        "store_state", "payment_method", "order_status"
    ])

    silver_sales_columns: List[str] = field(default_factory=lambda: [
        "transaction_id", "transaction_date", "customer_id", "customer_name",
        "customer_email", "customer_segment", "product_id", "product_name",
        "category", "sub_category", "brand", "unit_price", "quantity",
        "discount_pct", "total_amount", "net_amount", "store_id",
        "store_name", "store_city", "store_state", "payment_method",
        "order_status", "ingestion_date", "processing_timestamp"
    ])

    dim_customer_columns: List[str] = field(default_factory=lambda: [
        "customer_sk", "customer_id", "customer_name", "customer_email",
        "customer_segment", "effective_date", "end_date", "is_current"
    ])

    dim_product_columns: List[str] = field(default_factory=lambda: [
        "product_sk", "product_id", "product_name", "category",
        "sub_category", "brand", "current_price", "effective_date",
        "end_date", "is_current"
    ])

    dim_store_columns: List[str] = field(default_factory=lambda: [
        "store_sk", "store_id", "store_name", "store_city",
        "store_state", "effective_date", "end_date", "is_current"
    ])

    dim_date_columns: List[str] = field(default_factory=lambda: [
        "date_key", "full_date", "year", "quarter", "month",
        "month_name", "week_of_year", "day_of_month", "day_of_week",
        "day_name", "is_weekend", "is_holiday"
    ])

    fact_sales_columns: List[str] = field(default_factory=lambda: [
        "sales_sk", "transaction_id", "date_key", "customer_sk",
        "product_sk", "store_sk", "quantity", "unit_price",
        "discount_pct", "total_amount", "net_amount",
        "payment_method", "order_status", "processing_timestamp"
    ])


@dataclass
class IncrementalConfig:
    """Controls how incremental / delta loads work."""
    watermark_column: str = "transaction_date"
    watermark_format: str = "%Y-%m-%d"
    late_arrival_days: int = 3       # how far back we look for stragglers
    checkpoint_path: str = "/mnt/checkpoints/retail"


@dataclass
class QualityConfig:
    """
    Data quality thresholds.
    If any metric blows past these, we flag it during profiling.
    """
    max_null_pct: float = 0.05       # 5% — anything more is suspicious
    max_duplicate_pct: float = 0.01
    min_row_count: int = 100
    price_range: tuple = (0.01, 50000.00)
    quantity_range: tuple = (1, 10000)


@dataclass
class PipelineConfig:
    """Top-level config — bundles everything together."""
    storage: StorageConfig = field(default_factory=StorageConfig)
    schema: SchemaConfig = field(default_factory=SchemaConfig)
    incremental: IncrementalConfig = field(default_factory=IncrementalConfig)
    quality: QualityConfig = field(default_factory=QualityConfig)

    # Azure SQL — where the Gold tables end up
    sql_server: str = "retail-sql-server.database.windows.net"
    sql_database: str = "RetailDW"
    sql_schema: str = "dbo"

    # Spark tuning (reasonable defaults for ~5k-50k rows/day)
    spark_shuffle_partitions: int = 200
    coalesce_partitions: int = 8
    batch_size: int = 50000

    @property
    def run_id(self):
        return datetime.now().strftime("%Y%m%d_%H%M%S")
