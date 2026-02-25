#!/usr/bin/env python3
"""
Delta Lake Historical Seeder — Generates a large volume of historical
transaction data and writes it directly to the streaming Delta Lake table.

Pre-populates the table so batch processing can run immediately,
even before the streaming pipeline has accumulated real-time data.

Usage:
    cd scripts/
    uv sync
    uv run python delta_lake_seeder.py

    # Generate 5M records spanning 180 days:
    TOTAL_RECORDS=5000000 DATE_RANGE_DAYS=180 uv run python delta_lake_seeder.py

    # Write to a separate seed path:
    SEED_SEPARATE=true uv run python delta_lake_seeder.py
"""

import os
import random
import uuid
from datetime import datetime, timedelta, timezone

# Set SPARK_LOCAL_IP before PySpark imports to avoid UnknownHostException
# when the machine's hostname is not resolvable (e.g. not in /etc/hosts).
# This must happen before PySpark spawns the Java gateway process.
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

from delta import configure_spark_with_delta_pip
from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ─────────────────────────────────────────────
# Configuration (environment variables)
# ─────────────────────────────────────────────

# Resolve project-relative default paths (scripts/ -> project root -> data/delta/...)
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
_DEFAULT_DELTA_BASE = os.path.join(_PROJECT_ROOT, "data", "delta")

TOTAL_RECORDS = int(os.environ.get("TOTAL_RECORDS", "1000000"))
DATE_RANGE_DAYS = int(os.environ.get("DATE_RANGE_DAYS", "90"))
DELTA_OUTPUT_PATH = os.environ.get(
    "DELTA_OUTPUT_PATH", os.path.join(_DEFAULT_DELTA_BASE, "stream", "transactions")
)
SEED_SEPARATE = os.environ.get("SEED_SEPARATE", "false").lower() == "true"
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")

if SEED_SEPARATE:
    DELTA_OUTPUT_PATH = os.environ.get(
        "DELTA_SEED_PATH", os.path.join(_DEFAULT_DELTA_BASE, "seed", "transactions")
    )

# ─────────────────────────────────────────────
# Data Distributions (same as kafka_stream_generator)
# ─────────────────────────────────────────────

CATEGORIES = {
    "electronics": {
        "weight": 0.20,
        "price_range": (29.99, 2499.99),
        "sub_cats": [
            "smartphones",
            "laptops",
            "audio_headphones",
            "cameras",
            "tablets",
            "wearables",
        ],
    },
    "clothing": {
        "weight": 0.18,
        "price_range": (9.99, 299.99),
        "sub_cats": [
            "mens_shirts",
            "womens_dresses",
            "shoes",
            "activewear",
            "accessories",
        ],
    },
    "grocery": {
        "weight": 0.22,
        "price_range": (1.99, 89.99),
        "sub_cats": ["dairy", "snacks", "beverages", "frozen", "organic", "bakery"],
    },
    "home_garden": {
        "weight": 0.10,
        "price_range": (14.99, 999.99),
        "sub_cats": ["furniture", "kitchen", "bedding", "garden_tools", "lighting"],
    },
    "sports": {
        "weight": 0.08,
        "price_range": (19.99, 799.99),
        "sub_cats": ["fitness", "outdoor", "team_sports", "cycling", "swimming"],
    },
    "beauty": {
        "weight": 0.07,
        "price_range": (4.99, 199.99),
        "sub_cats": ["skincare", "makeup", "haircare", "fragrance", "supplements"],
    },
    "automotive": {
        "weight": 0.04,
        "price_range": (9.99, 499.99),
        "sub_cats": ["parts", "accessories", "tools", "car_care", "electronics"],
    },
    "books": {
        "weight": 0.05,
        "price_range": (5.99, 49.99),
        "sub_cats": ["fiction", "non_fiction", "textbooks", "children", "comics"],
    },
    "toys": {
        "weight": 0.03,
        "price_range": (7.99, 199.99),
        "sub_cats": [
            "action_figures",
            "board_games",
            "educational",
            "outdoor",
            "dolls",
        ],
    },
    "pharmacy": {
        "weight": 0.03,
        "price_range": (2.99, 149.99),
        "sub_cats": [
            "otc_medicine",
            "vitamins",
            "first_aid",
            "personal_care",
            "baby_care",
        ],
    },
}

REGIONS = {
    "north": {
        "cities": ["Delhi", "Chandigarh", "Lucknow", "Jaipur"],
        "states": ["DL", "CH", "UP", "RJ"],
        "weight": 0.22,
    },
    "south": {
        "cities": ["Bangalore", "Chennai", "Hyderabad", "Kochi"],
        "states": ["KA", "TN", "TS", "KL"],
        "weight": 0.25,
    },
    "east": {
        "cities": ["Kolkata", "Patna", "Bhubaneswar", "Guwahati"],
        "states": ["WB", "BR", "OD", "AS"],
        "weight": 0.12,
    },
    "west": {
        "cities": ["Mumbai", "Pune", "Ahmedabad", "Goa"],
        "states": ["MH", "MH", "GJ", "GA"],
        "weight": 0.28,
    },
    "central": {
        "cities": ["Bhopal", "Nagpur", "Indore", "Raipur"],
        "states": ["MP", "MH", "MP", "CG"],
        "weight": 0.08,
    },
    "northeast": {
        "cities": ["Guwahati", "Imphal", "Shillong", "Agartala"],
        "states": ["AS", "MN", "ML", "TR"],
        "weight": 0.05,
    },
}

PAYMENT_METHODS = [
    "credit_card",
    "debit_card",
    "upi",
    "wallet",
    "bank_transfer",
    "cod",
    "gift_card",
]
PAYMENT_WEIGHTS = [0.30, 0.20, 0.25, 0.10, 0.05, 0.08, 0.02]

CHANNELS = ["pos_in_store", "web", "mobile_app", "marketplace", "call_center"]
CHANNEL_WEIGHTS = [0.25, 0.20, 0.35, 0.15, 0.05]

CUSTOMER_TIERS = ["bronze", "silver", "gold", "platinum"]
TIER_WEIGHTS = [0.50, 0.30, 0.15, 0.05]

CARD_NETWORKS = ["visa", "mastercard", "amex", "rupay", "discover"]
CARD_WEIGHTS = [0.35, 0.30, 0.15, 0.15, 0.05]

BRANDS = {
    "electronics": ["Apple", "Samsung", "Sony", "LG", "Dell", "HP", "Bose", "OnePlus"],
    "clothing": ["Nike", "Adidas", "Zara", "H&M", "Levi's", "Puma", "Uniqlo"],
    "grocery": ["Amul", "Nestle", "Britannia", "ITC", "Parle", "Haldiram"],
    "home_garden": ["IKEA", "HomeTown", "Godrej", "Nilkamal", "Wipro"],
    "sports": ["Nike", "Adidas", "Puma", "Decathlon", "Yonex", "Wilson"],
    "beauty": ["Lakme", "L'Oreal", "Maybelline", "Nivea", "Dove"],
    "automotive": ["Bosch", "3M", "Castrol", "Shell", "Philips"],
    "books": ["Penguin", "HarperCollins", "Scholastic", "Oxford", "Cambridge"],
    "toys": ["Lego", "Hasbro", "Mattel", "Funskool", "PlayDoh"],
    "pharmacy": ["Cipla", "Sun Pharma", "Himalaya", "Dabur", "Patanjali"],
}

FRAUD_RATE = 0.02
fake = Faker()


# ─────────────────────────────────────────────
# Pool Generation (same as kafka generator)
# ─────────────────────────────────────────────


def generate_customer_pool(n: int) -> list:
    customers = []
    for i in range(n):
        tier = random.choices(CUSTOMER_TIERS, weights=TIER_WEIGHTS, k=1)[0]
        region = random.choices(
            list(REGIONS.keys()), weights=[r["weight"] for r in REGIONS.values()], k=1
        )[0]
        channel = random.choices(CHANNELS, weights=CHANNEL_WEIGHTS, k=1)[0]
        customers.append(
            {
                "customer_id": f"CUST-{i:08d}",
                "customer_tier": tier,
                "home_region": region,
                "preferred_channel": channel,
            }
        )
    return customers


def generate_product_pool(n: int) -> list:
    products = []
    cat_names = list(CATEGORIES.keys())
    cat_weights = [CATEGORIES[c]["weight"] for c in cat_names]
    for i in range(n):
        cat = random.choices(cat_names, weights=cat_weights, k=1)[0]
        cat_info = CATEGORIES[cat]
        sub_cat = random.choice(cat_info["sub_cats"])
        brand = random.choice(BRANDS.get(cat, ["Generic"]))
        price = round(random.uniform(*cat_info["price_range"]), 2)
        products.append(
            {
                "product_id": f"SKU-{cat[:3].upper()}-{i:05d}",
                "product_name": f"{brand} {sub_cat.replace('_', ' ').title()} #{i}",
                "category": cat,
                "sub_category": sub_cat,
                "brand": brand,
                "unit_price": price,
            }
        )
    return products


def generate_store_pool(n: int) -> list:
    stores = []
    for i in range(n):
        region = random.choices(
            list(REGIONS.keys()), weights=[r["weight"] for r in REGIONS.values()], k=1
        )[0]
        region_info = REGIONS[region]
        city_idx = random.randrange(len(region_info["cities"]))
        stores.append(
            {
                "store_id": f"STORE-{region_info['cities'][city_idx][:3].upper()}-{i:03d}",
                "region": region,
                "city": region_info["cities"][city_idx],
                "state": region_info["states"][city_idx],
            }
        )
    return stores


# ─────────────────────────────────────────────
# Record Generation
# ─────────────────────────────────────────────


def generate_record(customers, products, stores, start_date, end_date) -> dict:
    """Generate a single historical transaction record."""
    customer = random.choice(customers)
    product = random.choice(products)

    # Random timestamp within the date range, weighted toward business hours
    days_diff = (end_date - start_date).days
    random_day = start_date + timedelta(days=random.randint(0, days_diff))
    is_weekend = random_day.weekday() >= 5

    # Business hours weighting (8 AM - 10 PM more likely)
    if random.random() < 0.8:
        hour = random.randint(8, 22)
    else:
        hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    microsecond = random.randint(0, 999999)
    event_timestamp = random_day.replace(
        hour=hour,
        minute=minute,
        second=second,
        microsecond=microsecond,
        tzinfo=timezone.utc,
    )

    # Seasonality: weekends vs weekdays
    cat = product["category"]
    if is_weekend and cat in ("grocery", "clothing") and random.random() < 0.4:
        # Boost weekend grocery/clothing
        pass  # Already selected; just don't re-roll
    elif not is_weekend and cat == "electronics" and random.random() < 0.3:
        pass  # Boost weekday electronics

    # Channel
    if random.random() < 0.7:
        channel = customer["preferred_channel"]
    else:
        channel = random.choices(CHANNELS, weights=CHANNEL_WEIGHTS, k=1)[0]

    is_digital = channel in ("web", "mobile_app", "marketplace")
    if is_digital:
        store = None
        device_type = random.choice(["android", "ios", "desktop", "tablet"])
        session_id = f"sess-{uuid.uuid4().hex[:12]}"
    else:
        store = random.choice(stores)
        device_type = None
        session_id = None

    if store:
        region = store["region"]
        city = store["city"]
        state = store["state"]
        store_id = store["store_id"]
    else:
        region_name = customer["home_region"]
        region_info = REGIONS[region_name]
        city_idx = random.randrange(len(region_info["cities"]))
        region = region_name
        city = region_info["cities"][city_idx]
        state = region_info["states"][city_idx]
        store_id = None

    # Quantity
    qty_roll = random.random()
    if qty_roll < 0.70:
        quantity = 1
    elif qty_roll < 0.90:
        quantity = 2
    else:
        quantity = random.randint(3, 5)

    # Discount
    disc_roll = random.random()
    if disc_roll < 0.60:
        discount_percent = 0.0
    elif disc_roll < 0.90:
        discount_percent = round(random.uniform(5, 15), 1)
    else:
        discount_percent = round(random.uniform(20, 50), 1)

    unit_price = product["unit_price"]
    total_amount = round(quantity * unit_price * (1 - discount_percent / 100), 2)
    tax_amount = round(total_amount * 0.09, 2)

    payment_method = random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS, k=1)[0]
    card_network = None
    if payment_method in ("credit_card", "debit_card"):
        card_network = random.choices(CARD_NETWORKS, weights=CARD_WEIGHTS, k=1)[0]

    status_roll = random.random()
    if status_roll < 0.92:
        transaction_status = "completed"
    elif status_roll < 0.96:
        transaction_status = "pending"
    elif status_roll < 0.98:
        transaction_status = "failed"
    elif status_roll < 0.995:
        transaction_status = "refunded"
    else:
        transaction_status = "chargeback"

    if random.random() < FRAUD_RATE:
        is_fraudulent = True
        fraud_score = round(random.uniform(0.7, 1.0), 4)
    else:
        is_fraudulent = False
        fraud_score = round(random.uniform(0.0, 0.15), 4)

    postal_code = f"{random.randint(100000, 999999)}"

    return {
        "transaction_id": str(uuid.uuid4()),
        "event_timestamp": event_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "processing_timestamp": event_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        + "Z",
        "customer_id": customer["customer_id"],
        "customer_tier": customer["customer_tier"],
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "sub_category": product["sub_category"],
        "brand": product["brand"],
        "quantity": quantity,
        "unit_price": unit_price,
        "discount_percent": discount_percent,
        "total_amount": total_amount,
        "tax_amount": tax_amount,
        "currency": "USD",
        "payment_method": payment_method,
        "card_network": card_network,
        "transaction_status": transaction_status,
        "channel": channel,
        "store_id": store_id,
        "region": region,
        "city": city,
        "state": state,
        "postal_code": postal_code,
        "device_type": device_type,
        "session_id": session_id,
        "ip_address": f"{random.randint(1, 255)}.{random.randint(0, 255)}.xxx.xxx",
        "is_fraudulent": is_fraudulent,
        "fraud_score": fraud_score,
        "batch_id": None,
    }


# ─────────────────────────────────────────────
# PySpark Schema
# ─────────────────────────────────────────────

TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField(
            "event_timestamp", StringType(), False
        ),  # Will be cast to Timestamp
        StructField("processing_timestamp", StringType(), True),
        StructField("customer_id", StringType(), False),
        StructField("customer_tier", StringType(), True),
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("category", StringType(), False),
        StructField("sub_category", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DoubleType(), False),
        StructField("discount_percent", DoubleType(), True),
        StructField("total_amount", DoubleType(), False),
        StructField("tax_amount", DoubleType(), True),
        StructField("currency", StringType(), False),
        StructField("payment_method", StringType(), False),
        StructField("card_network", StringType(), True),
        StructField("transaction_status", StringType(), False),
        StructField("channel", StringType(), False),
        StructField("store_id", StringType(), True),
        StructField("region", StringType(), False),
        StructField("city", StringType(), False),
        StructField("state", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("is_fraudulent", BooleanType(), True),
        StructField("fraud_score", DoubleType(), True),
        StructField("batch_id", StringType(), True),
    ]
)


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────


def main():
    print(f"{'=' * 60}")
    print(f"  Delta Lake Historical Seeder")
    print(f"  Records:    {TOTAL_RECORDS:,}")
    print(f"  Date Range: {DATE_RANGE_DAYS} days")
    print(f"  Output:     {DELTA_OUTPUT_PATH}")
    print(f"  Separate:   {SEED_SEPARATE}")
    print(f"{'=' * 60}")

    # Initialize PySpark with Delta Lake
    # Use configure_spark_with_delta_pip to ensure Delta JARs are on the classpath
    builder = (
        SparkSession.builder.appName("DeltaLakeSeeder")
        .master(SPARK_MASTER)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Generate pools
    print("\nGenerating data pools...")
    customers = generate_customer_pool(5000)
    products = generate_product_pool(500)
    stores = generate_store_pool(50)
    print(
        f"  Customers: {len(customers)}, Products: {len(products)}, Stores: {len(stores)}"
    )

    # Date range
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=DATE_RANGE_DAYS)
    print(f"  Date range: {start_date.date()} to {end_date.date()}")

    # Generate records in batches to avoid memory issues
    BATCH_SIZE = min(100_000, TOTAL_RECORDS)
    records_written = 0

    print(f"\nGenerating {TOTAL_RECORDS:,} records in batches of {BATCH_SIZE:,}...")

    while records_written < TOTAL_RECORDS:
        current_batch_size = min(BATCH_SIZE, TOTAL_RECORDS - records_written)
        records = [
            generate_record(customers, products, stores, start_date, end_date)
            for _ in range(current_batch_size)
        ]

        df = spark.createDataFrame(records, schema=TRANSACTION_SCHEMA)

        # Cast timestamp strings to actual timestamps and add event_date
        df = (
            df.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
            .withColumn("processing_timestamp", F.to_timestamp("processing_timestamp"))
            .withColumn("event_date", F.to_date("event_timestamp"))
        )

        # Write to Delta Lake
        df.write.format("delta").partitionBy("event_date").mode("append").save(
            DELTA_OUTPUT_PATH
        )

        records_written += current_batch_size
        pct = (records_written / TOTAL_RECORDS) * 100
        print(f"  Written {records_written:,} / {TOTAL_RECORDS:,} ({pct:.1f}%)")

    # Print summary statistics
    print(f"\n{'=' * 60}")
    print("  Summary Statistics")
    print(f"{'=' * 60}")

    result_df = spark.read.format("delta").load(DELTA_OUTPUT_PATH)
    total_count = result_df.count()
    print(f"  Total records in Delta table: {total_count:,}")

    print("\n  Records per category:")
    cat_counts = (
        result_df.groupBy("category").count().orderBy(F.desc("count")).collect()
    )
    for row in cat_counts:
        print(f"    {row['category']:20s} {row['count']:>10,}")

    stats = result_df.agg(
        F.min("event_timestamp").alias("min_date"),
        F.max("event_timestamp").alias("max_date"),
        F.avg("total_amount").alias("avg_order_value"),
        F.avg(F.col("is_fraudulent").cast("int")).alias("fraud_rate"),
    ).collect()[0]

    print(f"\n  Date range:        {stats['min_date']} to {stats['max_date']}")
    print(f"  Avg order value:   ${stats['avg_order_value']:.2f}")
    print(f"  Fraud rate:        {stats['fraud_rate'] * 100:.2f}%")
    print(f"\n{'=' * 60}")
    print("  Seeding complete!")
    print(f"{'=' * 60}")

    spark.stop()


if __name__ == "__main__":
    main()
