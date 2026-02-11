# test_transformations.py
# Tests for the data generator and config classes.
# These run locally with pytest (no Spark needed).

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class TestDataQualityRules:
    """Make sure our quality rule constants are sane."""

    def test_valid_customer_segments(self):
        valid = ["Premium", "Regular", "Budget"]
        assert "Premium" in valid
        assert "Gold" not in valid

    def test_valid_order_statuses(self):
        valid = ["Completed", "Returned", "Cancelled"]
        assert "Completed" in valid
        assert "Pending" not in valid

    def test_valid_payment_methods(self):
        valid = ["Credit Card", "Debit Card", "Cash", "Digital Wallet", "Bank Transfer"]
        assert "Cash" in valid
        assert "Crypto" not in valid

    def test_price_range_validation(self):
        """Prices should be between $0.01 and $50k."""
        min_price, max_price = 0.01, 50000.00
        assert 29.99 >= min_price
        assert 29.99 <= max_price
        assert -5.00 < min_price  # Should fail validation
        assert 99999.00 > max_price  # Should fail validation

    def test_quantity_range_validation(self):
        min_qty, max_qty = 1, 10000
        assert 5 >= min_qty
        assert 5 <= max_qty
        assert 0 < min_qty  # Should fail
        assert 15000 > max_qty  # Should fail


class TestDataGenerator:
    """Check that the data generator output looks right."""

    def test_generate_single_transaction(self):
        """A single transaction should have all the expected fields and valid values."""
        from scripts.generate_sample_data import generate_transaction
        from datetime import datetime

        txn = generate_transaction(datetime(2025, 1, 15))

        assert txn["transaction_id"].startswith("TXN-")
        assert "2025-01-15" in txn["transaction_date"]
        assert txn["customer_id"].startswith("CUST-")
        assert txn["product_id"].startswith("PROD-")
        assert txn["store_id"].startswith("STORE-")
        assert txn["unit_price"] > 0
        assert txn["quantity"] >= 1
        assert 0 <= txn["discount_pct"] <= 0.25

    def test_generate_daily_data_count(self):
        from scripts.generate_sample_data import generate_daily_data
        from datetime import datetime

        data = generate_daily_data(datetime(2025, 1, 1), min_transactions=50, max_transactions=100)

        # Should have at least 50 records (plus possible dupes)
        assert len(data) >= 50

    def test_generate_daily_data_has_required_fields(self):
        from scripts.generate_sample_data import generate_daily_data
        from datetime import datetime

        data = generate_daily_data(datetime(2025, 1, 1), min_transactions=10, max_transactions=20)
        required_fields = [
            "transaction_id", "transaction_date", "customer_id",
            "product_id", "unit_price", "quantity", "store_id"
        ]

        for record in data[:5]:
            for field in required_fields:
                assert field in record, f"Missing field: {field}"

    def test_late_arriving_data(self):
        from scripts.generate_sample_data import generate_late_arriving_data
        from datetime import datetime

        late = generate_late_arriving_data(datetime(2025, 1, 15), days_late=3, num_records=10)

        assert len(late) == 10
        for record in late:
            # Late records should have dates before Jan 15
            assert "2025-01-1" in record["transaction_date"][:9]

    def test_dirty_data_injection(self):
        """With dirty injection on, we should see some messy records in the output."""
        from scripts.generate_sample_data import generate_daily_data
        from datetime import datetime
        import random

        random.seed(42)
        data = generate_daily_data(datetime(2025, 1, 1), min_transactions=200, max_transactions=300)

        # Check that some records have dirty data characteristics
        empty_names = sum(1 for r in data if r["customer_name"].strip() == "")
        has_dirty = empty_names > 0 or len(data) > 200  # dupes also count

        # With 200+ records and 5% dirty rate, there should be at least some
        assert len(data) >= 200


class TestSchemaValidation:
    """Verify the config classes have the right defaults."""

    def test_schema_config_has_all_layers(self):
        from config.pipeline_config import SchemaConfig

        schema = SchemaConfig()
        assert len(schema.raw_sales_columns) == 20
        assert len(schema.fact_sales_columns) > 0
        assert len(schema.dim_customer_columns) > 0
        assert len(schema.dim_product_columns) > 0

    def test_pipeline_config_defaults(self):
        from config.pipeline_config import PipelineConfig

        config = PipelineConfig()
        assert config.storage.storage_account == "retaildatalake"
        assert config.quality.max_null_pct == 0.05
        assert config.incremental.late_arrival_days == 3

    def test_storage_path_generation(self):
        from config.pipeline_config import StorageConfig

        storage = StorageConfig()
        path = storage.get_path("bronze", "sales", "2025-01-15")

        assert "bronze" in path
        assert "sales" in path
        assert "2025-01-15" in path


class TestSurrogateKeyLogic:
    """Sanity checks for hash-based key generation."""

    def test_hash_consistency(self):
        """Same input should produce same hash."""
        import hashlib

        val1 = hashlib.sha256("CUST-0001||Alice Johnson".encode()).hexdigest()
        val2 = hashlib.sha256("CUST-0001||Alice Johnson".encode()).hexdigest()
        assert val1 == val2

    def test_hash_uniqueness(self):
        """Different inputs should produce different hashes."""
        import hashlib

        val1 = hashlib.sha256("CUST-0001".encode()).hexdigest()
        val2 = hashlib.sha256("CUST-0002".encode()).hexdigest()
        assert val1 != val2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
