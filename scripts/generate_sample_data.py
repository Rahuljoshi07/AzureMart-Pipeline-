# generate_sample_data.py
# Creates fake-but-realistic retail sales data to feed into the pipeline.
# Spits out date-partitioned CSVs that look like what we'd get from a real POS system.

import csv
import os
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

# --- Reference / lookup data ---

CUSTOMERS = [
    {"id": f"CUST-{i:04d}", "name": name, "email": f"{name.lower().replace(' ', '.')}@example.com", "segment": seg}
    for i, (name, seg) in enumerate([
        ("Alice Johnson", "Premium"), ("Bob Smith", "Regular"), ("Carol Davis", "Premium"),
        ("David Wilson", "Budget"), ("Eve Martinez", "Regular"), ("Frank Brown", "Premium"),
        ("Grace Lee", "Budget"), ("Henry Taylor", "Regular"), ("Irene Anderson", "Premium"),
        ("Jack Thomas", "Budget"), ("Karen White", "Regular"), ("Leo Harris", "Premium"),
        ("Mia Clark", "Regular"), ("Noah Lewis", "Budget"), ("Olivia Walker", "Premium"),
        ("Paul Hall", "Regular"), ("Quinn Allen", "Budget"), ("Rita Young", "Premium"),
        ("Sam King", "Regular"), ("Tina Wright", "Budget"), ("Uma Lopez", "Premium"),
        ("Victor Hill", "Regular"), ("Wendy Scott", "Budget"), ("Xander Green", "Premium"),
        ("Yara Adams", "Regular"), ("Zane Baker", "Budget"), ("Amber Nelson", "Premium"),
        ("Brian Carter", "Regular"), ("Clara Mitchell", "Budget"), ("Derek Perez", "Premium"),
    ], start=1)
]

PRODUCTS = [
    {"id": "PROD-0001", "name": "Laptop Pro 15", "category": "Electronics", "sub_category": "Laptops", "brand": "TechCorp", "price": 1299.99},
    {"id": "PROD-0002", "name": "Wireless Mouse", "category": "Electronics", "sub_category": "Accessories", "brand": "TechCorp", "price": 29.99},
    {"id": "PROD-0003", "name": "USB-C Hub", "category": "Electronics", "sub_category": "Accessories", "brand": "ConnectPro", "price": 49.99},
    {"id": "PROD-0004", "name": "Mechanical Keyboard", "category": "Electronics", "sub_category": "Accessories", "brand": "KeyMaster", "price": 149.99},
    {"id": "PROD-0005", "name": "4K Monitor 27\"", "category": "Electronics", "sub_category": "Monitors", "brand": "ViewMax", "price": 449.99},
    {"id": "PROD-0006", "name": "Standing Desk", "category": "Furniture", "sub_category": "Desks", "brand": "ErgoLife", "price": 599.99},
    {"id": "PROD-0007", "name": "Office Chair", "category": "Furniture", "sub_category": "Chairs", "brand": "ErgoLife", "price": 399.99},
    {"id": "PROD-0008", "name": "Desk Lamp LED", "category": "Furniture", "sub_category": "Lighting", "brand": "BrightCo", "price": 39.99},
    {"id": "PROD-0009", "name": "Notebook A5", "category": "Stationery", "sub_category": "Notebooks", "brand": "PaperWorks", "price": 12.99},
    {"id": "PROD-0010", "name": "Pen Set Premium", "category": "Stationery", "sub_category": "Pens", "brand": "PaperWorks", "price": 24.99},
    {"id": "PROD-0011", "name": "Webcam HD", "category": "Electronics", "sub_category": "Cameras", "brand": "ViewMax", "price": 79.99},
    {"id": "PROD-0012", "name": "Headphones NC", "category": "Electronics", "sub_category": "Audio", "brand": "SoundWave", "price": 249.99},
    {"id": "PROD-0013", "name": "Bluetooth Speaker", "category": "Electronics", "sub_category": "Audio", "brand": "SoundWave", "price": 89.99},
    {"id": "PROD-0014", "name": "Tablet 10\"", "category": "Electronics", "sub_category": "Tablets", "brand": "TechCorp", "price": 499.99},
    {"id": "PROD-0015", "name": "Phone Charger Fast", "category": "Electronics", "sub_category": "Accessories", "brand": "ConnectPro", "price": 34.99},
    {"id": "PROD-0016", "name": "Whiteboard 48x36", "category": "Stationery", "sub_category": "Boards", "brand": "OfficePro", "price": 89.99},
    {"id": "PROD-0017", "name": "Filing Cabinet", "category": "Furniture", "sub_category": "Storage", "brand": "OfficePro", "price": 199.99},
    {"id": "PROD-0018", "name": "Printer Laser", "category": "Electronics", "sub_category": "Printers", "brand": "PrintMax", "price": 349.99},
    {"id": "PROD-0019", "name": "Paper A4 Ream", "category": "Stationery", "sub_category": "Paper", "brand": "PaperWorks", "price": 8.99},
    {"id": "PROD-0020", "name": "Surge Protector", "category": "Electronics", "sub_category": "Accessories", "brand": "ConnectPro", "price": 19.99},
]

STORES = [
    {"id": "STORE-01", "name": "Downtown Flagship", "city": "New York", "state": "NY"},
    {"id": "STORE-02", "name": "Mall Central", "city": "Los Angeles", "state": "CA"},
    {"id": "STORE-03", "name": "Tech Hub", "city": "San Francisco", "state": "CA"},
    {"id": "STORE-04", "name": "Midtown Express", "city": "Chicago", "state": "IL"},
    {"id": "STORE-05", "name": "Harbor Point", "city": "Seattle", "state": "WA"},
    {"id": "STORE-06", "name": "Suburban Plaza", "city": "Austin", "state": "TX"},
    {"id": "STORE-07", "name": "East Side Store", "city": "Boston", "state": "MA"},
    {"id": "STORE-08", "name": "Lakefront Outlet", "city": "Denver", "state": "CO"},
]

PAYMENT_METHODS = ["Credit Card", "Debit Card", "Cash", "Digital Wallet", "Bank Transfer"]
# Weighted so most orders are completed (which is realistic)
ORDER_STATUSES = ["Completed", "Completed", "Completed", "Completed", "Returned", "Cancelled"]


def generate_transaction(transaction_date, inject_dirty=False):
    """Create one fake sales transaction."""
    customer = random.choice(CUSTOMERS)
    product = random.choice(PRODUCTS)
    store = random.choice(STORES)
    quantity = random.choices([1, 2, 3, 4, 5, 10], weights=[40, 25, 15, 10, 7, 3])[0]
    discount = random.choices([0, 0.05, 0.10, 0.15, 0.20, 0.25], weights=[40, 20, 15, 12, 8, 5])[0]

    record = {
        "transaction_id": f"TXN-{uuid.uuid4().hex[:12].upper()}",
        "transaction_date": transaction_date.strftime("%Y-%m-%d %H:%M:%S"),
        "customer_id": customer["id"],
        "customer_name": customer["name"],
        "customer_email": customer["email"],
        "customer_segment": customer["segment"],
        "product_id": product["id"],
        "product_name": product["name"],
        "category": product["category"],
        "sub_category": product["sub_category"],
        "brand": product["brand"],
        "unit_price": product["price"],
        "quantity": quantity,
        "discount_pct": discount,
        "store_id": store["id"],
        "store_name": store["name"],
        "store_city": store["city"],
        "store_state": store["state"],
        "payment_method": random.choice(PAYMENT_METHODS),
        "order_status": random.choice(ORDER_STATUSES),
    }

    # Sprinkle in some bad data (~5% of records) to test our cleaning logic
    if inject_dirty and random.random() < 0.05:
        dirty_type = random.choice(["null_name", "null_email", "dup_txn", "bad_price", "bad_qty", "extra_spaces"])
        if dirty_type == "null_name":
            record["customer_name"] = ""
        elif dirty_type == "null_email":
            record["customer_email"] = ""
        elif dirty_type == "bad_price":
            record["unit_price"] = -1 * product["price"]
        elif dirty_type == "bad_qty":
            record["quantity"] = 0
        elif dirty_type == "extra_spaces":
            record["product_name"] = f"  {product['name']}  "
            record["customer_name"] = f" {customer['name']} "

    return record


def generate_daily_data(
    date,
    min_transactions=80,
    max_transactions=250,
    inject_dirty=True,
):
    """Build a whole day's worth of transactions. Hourly weights simulate realistic traffic."""
    num_transactions = random.randint(min_transactions, max_transactions)
    transactions = []

    # More sales during business hours, fewer at night
    for _ in range(num_transactions):
        hour = random.choices(range(24), weights=[
            1, 1, 1, 1, 1, 2, 3, 5, 8, 10, 12, 11,
            13, 11, 10, 9, 8, 10, 12, 8, 5, 3, 2, 1
        ])[0]
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        txn_datetime = date.replace(hour=hour, minute=minute, second=second)
        transactions.append(generate_transaction(txn_datetime, inject_dirty))

    # Throw in a few duplicates too (~2%) — the silver layer should catch these
    if inject_dirty:
        num_dupes = max(1, int(len(transactions) * 0.02))
        for _ in range(num_dupes):
            transactions.append(random.choice(transactions).copy())

    random.shuffle(transactions)
    return transactions


def generate_late_arriving_data(current_date, days_late=3, num_records=15):
    """Fake some transactions that 'arrived late' from the past few days."""
    late_records = []
    for _ in range(num_records):
        late_days = random.randint(1, days_late)
        late_date = current_date - timedelta(days=late_days)
        record = generate_transaction(late_date, inject_dirty=False)
        record["_late_arriving"] = True
        late_records.append(record)
    return late_records


def write_csv(records, filepath):
    """Dump records to a CSV. Returns how many rows were written."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    fieldnames = list(records[0].keys())

    # Strip out any internal fields (like _late_arriving) before writing
    fieldnames = [f for f in fieldnames if not f.startswith("_")]

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)

    return len(records)


def generate_dataset(
    output_dir,
    start_date="2025-01-01",
    num_days=30,
    include_late_data=True,
):
    """Generate a full multi-day dataset. This is the main entry point."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    total_records = 0

    print(f"\nGenerating {num_days} days of retail data starting from {start_date}")
    print("-" * 55)

    for day_offset in range(num_days):
        current_date = start + timedelta(days=day_offset)
        date_str = current_date.strftime("%Y-%m-%d")
        date_partition = current_date.strftime("%Y/%m/%d")

        daily_data = generate_daily_data(current_date)

        # After the first few days, mix in some late arrivals
        if include_late_data and day_offset > 3:
            late_data = generate_late_arriving_data(current_date)
            daily_data.extend(late_data)

        filepath = os.path.join(output_dir, "raw", "sales", date_partition, f"sales_{date_str}.csv")
        count = write_csv(daily_data, filepath)
        total_records += count

        print(f"  {date_str}  {count:>4} records")

    # Also spit out one combined file — handy for quick testing
    print(f"\nBuilding combined sample file...")
    all_data = []
    for day_offset in range(min(7, num_days)):
        current_date = start + timedelta(days=day_offset)
        all_data.extend(generate_daily_data(current_date, min_transactions=50, max_transactions=100))

    combined_path = os.path.join(output_dir, "raw", "sales", "sales_combined_sample.csv")
    combined_count = write_csv(all_data, combined_path)

    print(f"  Combined sample: {combined_count} records")
    print(f"\nDone! {total_records} total records in {output_dir}")


if __name__ == "__main__":
    project_root = Path(__file__).parent.parent
    output = str(project_root / "data")

    generate_dataset(
        output_dir=output,
        start_date="2025-01-01",
        num_days=30,
        include_late_data=True,
    )
