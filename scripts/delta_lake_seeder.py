"""Delta Lake Historical Seeder — Generates a large volume of historical data.

Generates a large volume of historical transaction data and writes it directly
to the streaming Delta Lake table.

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

import logging
import os
import secrets
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Set SPARK_LOCAL_IP before PySpark imports to avoid UnknownHostException
# when the machine's hostname is not resolvable (e.g. not in /etc/hosts).
# This must happen before PySpark spawns the Java gateway process.
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

from delta import configure_spark_with_delta_pip
from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql import functions as spark_funcs
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ─────────────────────────────────────────────
# Configuration (environment variables)
# ─────────────────────────────────────────────

# Resolve project-relative default paths (scripts/ -> project root -> data/delta/...)
_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SCRIPT_DIR.parent
_DEFAULT_DELTA_BASE = _PROJECT_ROOT / "data" / "delta"

TOTAL_RECORDS = int(os.environ.get("TOTAL_RECORDS", "1000000"))
DATE_RANGE_DAYS = int(os.environ.get("DATE_RANGE_DAYS", "90"))
DELTA_OUTPUT_PATH = os.environ.get(
    "DELTA_OUTPUT_PATH",
    str(_DEFAULT_DELTA_BASE / "stream" / "transactions"),
)
SEED_SEPARATE = os.environ.get("SEED_SEPARATE", "false").lower() == "true"
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")

if SEED_SEPARATE:
    DELTA_OUTPUT_PATH = os.environ.get(
        "DELTA_SEED_PATH",
        str(_DEFAULT_DELTA_BASE / "seed" / "transactions"),
    )

random = secrets.SystemRandom()

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
    """Generate a pool of customers."""
    customers = []
    for i in range(n):
        tier = random.choices(CUSTOMER_TIERS, weights=TIER_WEIGHTS, k=1)[0]
        region = random.choices(
            list(REGIONS.keys()),
            weights=[r["weight"] for r in REGIONS.values()],
            k=1,
        )[0]
        channel = random.choices(CHANNELS, weights=CHANNEL_WEIGHTS, k=1)[0]
        customers.append(
            {
                "customer_id": f"CUST-{i:08d}",
                "customer_tier": tier,
                "home_region": region,
                "preferred_channel": channel,
            },
        )
    return customers


def generate_product_pool(n: int) -> list:
    """Generate a pool of products."""
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
            },
        )
    return products


def generate_store_pool(n: int) -> list:
    """Generate a pool of stores."""
    stores = []
    for i in range(n):
        region = random.choices(
            list(REGIONS.keys()),
            weights=[r["weight"] for r in REGIONS.values()],
            k=1,
        )[0]
        region_info = REGIONS[region]
        city_idx = random.randrange(len(region_info["cities"]))
        stores.append(
            {
                "store_id": (
                    f"STORE-{region_info['cities'][city_idx][:3].upper()}-{i:03d}"
                ),
                "region": region,
                "city": region_info["cities"][city_idx],
                "state": region_info["states"][city_idx],
            },
        )
    return stores


# ─────────────────────────────────────────────
# Record Generation
# ─────────────────────────────────────────────


def _generate_timestamp(start_date: datetime, end_date: datetime) -> datetime:
    days_diff = (end_date - start_date).days
    random_day = start_date + timedelta(days=random.randint(0, days_diff))
    business_hour_prob = 0.8
    hour = (
        random.randint(8, 22)
        if random.random() < business_hour_prob
        else random.randint(0, 23)
    )
    return random_day.replace(
        hour=hour,
        minute=random.randint(0, 59),
        second=random.randint(0, 59),
        microsecond=random.randint(0, 999999),
        tzinfo=timezone.utc,
    )


def _get_channel_and_store(customer: dict, stores: list) -> tuple:
    preferred_channel_prob = 0.7
    channel = (
        customer["preferred_channel"]
        if random.random() < preferred_channel_prob
        else random.choices(CHANNELS, weights=CHANNEL_WEIGHTS, k=1)[0]
    )
    is_digital = channel in ("web", "mobile_app", "marketplace")

    if is_digital:
        return (
            channel,
            None,
            random.choice(["android", "ios", "desktop", "tablet"]),
            f"sess-{uuid.uuid4().hex[:12]}",
        )
    return channel, random.choice(stores), None, None


def _get_location(store: dict | None, customer: dict) -> tuple:
    if store:
        return store["region"], store["city"], store["state"], store["store_id"]
    region_name = customer["home_region"]
    region_info = REGIONS[region_name]
    city_idx = random.randrange(len(region_info["cities"]))
    return (
        region_name,
        region_info["cities"][city_idx],
        region_info["states"][city_idx],
        None,
    )


def _get_financials(product: dict) -> tuple:
    qty_roll = random.random()
    qty_1_prob = 0.70
    qty_2_prob = 0.90
    quantity = (
        1
        if qty_roll < qty_1_prob
        else (2 if qty_roll < qty_2_prob else random.randint(3, 5))
    )

    disc_roll = random.random()
    disc_0_prob = 0.60
    disc_small_prob = 0.90
    discount_percent = (
        0.0
        if disc_roll < disc_0_prob
        else (
            round(random.uniform(5, 15), 1)
            if disc_roll < disc_small_prob
            else round(random.uniform(20, 50), 1)
        )
    )

    unit_price = product["unit_price"]
    total_amount = round(quantity * unit_price * (1 - discount_percent / 100), 2)
    return (
        quantity,
        discount_percent,
        unit_price,
        total_amount,
        round(total_amount * 0.09, 2),
    )


def _get_payment_and_status() -> tuple:
    payment_method = random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS, k=1)[0]
    card_network = (
        random.choices(CARD_NETWORKS, weights=CARD_WEIGHTS, k=1)[0]
        if payment_method in ("credit_card", "debit_card")
        else None
    )

    status_roll = random.random()
    completed_prob = 0.92
    pending_prob = 0.96
    failed_prob = 0.98
    refunded_prob = 0.995
    if status_roll < completed_prob:
        transaction_status = "completed"
    elif status_roll < pending_prob:
        transaction_status = "pending"
    elif status_roll < failed_prob:
        transaction_status = "failed"
    elif status_roll < refunded_prob:
        transaction_status = "refunded"
    else:
        transaction_status = "chargeback"

    return payment_method, card_network, transaction_status


def generate_record(
    customers: list,
    products: list,
    stores: list,
    start_date: datetime,
    end_date: datetime,
) -> dict:
    """Generate a single historical transaction record."""
    customer = random.choice(customers)
    product = random.choice(products)
    event_timestamp = _generate_timestamp(start_date, end_date)
    channel, store, device_type, session_id = _get_channel_and_store(customer, stores)
    region, city, state, store_id = _get_location(store, customer)
    quantity, discount_percent, unit_price, total_amount, tax_amount = _get_financials(
        product,
    )
    payment_method, card_network, transaction_status = _get_payment_and_status()

    is_fraudulent = random.random() < FRAUD_RATE
    fraud_score = round(
        random.uniform(0.7, 1.0) if is_fraudulent else random.uniform(0.0, 0.15),
        4,
    )
    timestamp_str = event_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    return {
        "transaction_id": str(uuid.uuid4()),
        "event_timestamp": timestamp_str,
        "processing_timestamp": timestamp_str,
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
        "postal_code": f"{random.randint(100000, 999999)}",
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
        StructField("transaction_id", StringType(), nullable=False),
        StructField(
            "event_timestamp",
            StringType(),
            nullable=False,
        ),  # Will be cast to Timestamp
        StructField("processing_timestamp", StringType(), nullable=True),
        StructField("customer_id", StringType(), nullable=False),
        StructField("customer_tier", StringType(), nullable=True),
        StructField("product_id", StringType(), nullable=False),
        StructField("product_name", StringType(), nullable=False),
        StructField("category", StringType(), nullable=False),
        StructField("sub_category", StringType(), nullable=True),
        StructField("brand", StringType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=False),
        StructField("unit_price", DoubleType(), nullable=False),
        StructField("discount_percent", DoubleType(), nullable=True),
        StructField("total_amount", DoubleType(), nullable=False),
        StructField("tax_amount", DoubleType(), nullable=True),
        StructField("currency", StringType(), nullable=False),
        StructField("payment_method", StringType(), nullable=False),
        StructField("card_network", StringType(), nullable=True),
        StructField("transaction_status", StringType(), nullable=False),
        StructField("channel", StringType(), nullable=False),
        StructField("store_id", StringType(), nullable=True),
        StructField("region", StringType(), nullable=False),
        StructField("city", StringType(), nullable=False),
        StructField("state", StringType(), nullable=True),
        StructField("postal_code", StringType(), nullable=True),
        StructField("device_type", StringType(), nullable=True),
        StructField("session_id", StringType(), nullable=True),
        StructField("ip_address", StringType(), nullable=True),
        StructField("is_fraudulent", BooleanType(), nullable=True),
        StructField("fraud_score", DoubleType(), nullable=True),
        StructField("batch_id", StringType(), nullable=True),
    ],
)


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────


def main() -> None:
    """Run the Delta Lake seeder."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logger = logging.getLogger(__name__)

    logger.info("%s", "=" * 60)
    logger.info("  Delta Lake Historical Seeder")
    logger.info("  Records:    %s", f"{TOTAL_RECORDS:,}")
    logger.info("  Date Range: %d days", DATE_RANGE_DAYS)
    logger.info("  Output:     %s", DELTA_OUTPUT_PATH)
    logger.info("  Separate:   %s", SEED_SEPARATE)
    logger.info("%s", "=" * 60)

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
    logger.info("\nGenerating data pools...")
    customers = generate_customer_pool(5000)
    products = generate_product_pool(500)
    stores = generate_store_pool(50)
    logger.info(
        "  Customers: %d, Products: %d, Stores: %d",
        len(customers),
        len(products),
        len(stores),
    )

    # Date range
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=DATE_RANGE_DAYS)
    logger.info("  Date range: %s to %s", start_date.date(), end_date.date())

    # Generate records in batches to avoid memory issues
    batch_size = min(100_000, TOTAL_RECORDS)
    records_written = 0

    logger.info(
        "\nGenerating %s records in batches of %s...",
        f"{TOTAL_RECORDS:,}",
        f"{batch_size:,}",
    )

    while records_written < TOTAL_RECORDS:
        current_batch_size = min(batch_size, TOTAL_RECORDS - records_written)
        records = [
            generate_record(customers, products, stores, start_date, end_date)
            for _ in range(current_batch_size)
        ]

        df = spark.createDataFrame(records, schema=TRANSACTION_SCHEMA)

        # Cast timestamp strings to actual timestamps and add event_date
        df = (
            df.withColumn(
                "event_timestamp",
                spark_funcs.to_timestamp("event_timestamp"),
            )
            .withColumn(
                "processing_timestamp",
                spark_funcs.to_timestamp("processing_timestamp"),
            )
            .withColumn("event_date", spark_funcs.to_date("event_timestamp"))
        )

        # Write to Delta Lake
        df.write.format("delta").partitionBy("event_date").mode("append").save(
            DELTA_OUTPUT_PATH,
        )

        records_written += current_batch_size
        pct = (records_written / TOTAL_RECORDS) * 100
        logger.info(
            "  Written %s / %s (%.1f%%)",
            f"{records_written:,}",
            f"{TOTAL_RECORDS:,}",
            pct,
        )

    # Print summary statistics
    logger.info("\n%s", "=" * 60)
    logger.info("  Summary Statistics")
    logger.info("%s", "=" * 60)

    result_df = spark.read.format("delta").load(DELTA_OUTPUT_PATH)
    total_count = result_df.count()
    logger.info("  Total records in Delta table: %s", f"{total_count:,}")

    logger.info("\n  Records per category:")
    cat_counts = (
        result_df.groupBy("category")
        .count()
        .orderBy(spark_funcs.desc("count"))
        .collect()
    )
    for row in cat_counts:
        logger.info("    %-20s %10s", row["category"], f"{row['count']:,}")

    stats = result_df.agg(
        spark_funcs.min("event_timestamp").alias("min_date"),
        spark_funcs.max("event_timestamp").alias("max_date"),
        spark_funcs.avg("total_amount").alias("avg_order_value"),
        spark_funcs.avg(spark_funcs.col("is_fraudulent").cast("int")).alias(
            "fraud_rate",
        ),
    ).collect()[0]

    logger.info("\n  Date range:        %s to %s", stats["min_date"], stats["max_date"])
    logger.info("  Avg order value:   $%.2f", stats["avg_order_value"])
    logger.info("  Fraud rate:        %.2f%%", stats["fraud_rate"] * 100)
    logger.info("\n%s", "=" * 60)
    logger.info("  Seeding complete!")
    logger.info("%s", "=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
