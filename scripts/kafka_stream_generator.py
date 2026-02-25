"""Kafka Stream Generator — Produces realistic TransactionEvent JSON records.

Produces realistic TransactionEvent JSON records to the Kafka
'transactions-stream' topic.

Simulates real-world POS/e-commerce traffic with configurable throughput,
category distributions, fraud injection, and time-of-day patterns.

Usage:
    cd scripts/
    uv sync
    uv run python kafka_stream_generator.py

    # Or with custom settings:
    EVENTS_PER_SECOND=50 uv run python kafka_stream_generator.py
"""

import json
import logging
import os
import secrets
import signal
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer

# ─────────────────────────────────────────────
# Configuration (environment variables)
# ─────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "transactions-stream")
EVENTS_PER_SECOND = int(os.environ.get("EVENTS_PER_SECOND", "10"))
FRAUD_RATE = float(os.environ.get("FRAUD_RATE", "0.02"))
NUM_CUSTOMERS = int(os.environ.get("NUM_CUSTOMERS", "5000"))
NUM_PRODUCTS = int(os.environ.get("NUM_PRODUCTS", "500"))
NUM_STORES = int(os.environ.get("NUM_STORES", "50"))

# ─────────────────────────────────────────────
# Data Distributions
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
        "weight": 0.22,
        "states": ["DL", "CH", "UP", "RJ"],
    },
    "south": {
        "cities": ["Bangalore", "Chennai", "Hyderabad", "Kochi"],
        "weight": 0.25,
        "states": ["KA", "TN", "TS", "KL"],
    },
    "east": {
        "cities": ["Kolkata", "Patna", "Bhubaneswar", "Guwahati"],
        "weight": 0.12,
        "states": ["WB", "BR", "OD", "AS"],
    },
    "west": {
        "cities": ["Mumbai", "Pune", "Ahmedabad", "Goa"],
        "weight": 0.28,
        "states": ["MH", "MH", "GJ", "GA"],
    },
    "central": {
        "cities": ["Bhopal", "Nagpur", "Indore", "Raipur"],
        "weight": 0.08,
        "states": ["MP", "MH", "MP", "CG"],
    },
    "northeast": {
        "cities": ["Guwahati", "Imphal", "Shillong", "Agartala"],
        "weight": 0.05,
        "states": ["AS", "MN", "ML", "TR"],
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

fake = Faker()
random = secrets.SystemRandom()

# ─────────────────────────────────────────────
# Pool Generation
# ─────────────────────────────────────────────


def generate_customer_pool(n: int) -> list:
    """Pre-generate a pool of customers with assigned tiers, regions, and channels."""
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
    """Pre-generate a pool of products with assigned categories, brands, and prices."""
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
    """Pre-generate a pool of stores with assigned regions and cities."""
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
# Event Generation
# ─────────────────────────────────────────────


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


def generate_event(customers: list, products: list, stores: list) -> dict:
    """Generate a single TransactionEvent."""
    customer = random.choice(customers)
    product = random.choice(products)

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

    return {
        "transaction_id": str(uuid.uuid4()),
        "event_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[
            :-3
        ]
        + "Z",
        "processing_timestamp": None,
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
# Main Loop
# ─────────────────────────────────────────────


def main() -> None:
    """Run the Kafka stream generator."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logger = logging.getLogger(__name__)

    logger.info("%s", "=" * 60)
    logger.info("  Kafka Stream Generator")
    logger.info("  Bootstrap: %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("  Topic:     %s", KAFKA_TOPIC)
    logger.info("  Rate:      %s events/sec", EVENTS_PER_SECOND)
    logger.info("  Fraud:     %.1f%%", FRAUD_RATE * 100)
    logger.info("%s", "=" * 60)

    # Generate pools
    logger.info("Generating customer pool...")
    customers = generate_customer_pool(NUM_CUSTOMERS)
    logger.info("Generated %d customers", len(customers))

    logger.info("Generating product pool...")
    products = generate_product_pool(NUM_PRODUCTS)
    logger.info("Generated %d products", len(products))

    logger.info("Generating store pool...")
    stores = generate_store_pool(NUM_STORES)
    logger.info("Generated %d stores", len(stores))

    # Create Kafka producer
    logger.info("\nConnecting to Kafka at %s...", KAFKA_BOOTSTRAP_SERVERS)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    logger.info("Connected!\n")

    # Graceful shutdown
    running = True

    def signal_handler(_sig: int, _frame: object) -> None:
        nonlocal running
        logger.info("\n\nShutting down gracefully...")
        running = False

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    count = 0
    interval = 1.0 / EVENTS_PER_SECOND
    start_time = time.time()

    while running:
        event = generate_event(customers, products, stores)
        producer.send(
            KAFKA_TOPIC,
            key=event["transaction_id"],
            value=event,
        )
        count += 1

        if count % 100 == 0:
            elapsed = time.time() - start_time
            rate = count / elapsed if elapsed > 0 else 0
            fraud_marker = " ⚠️ FRAUD" if event["is_fraudulent"] else ""
            logger.info(
                "  Sent %s events (%.1f/sec) | Last: %s/%s $%.2f [%s]%s",
                f"{count:,}",
                rate,
                event["category"],
                event["product_name"][:30],
                event["total_amount"],
                event["transaction_status"],
                fraud_marker,
            )

        time.sleep(interval)

    producer.flush()
    producer.close()
    elapsed = time.time() - start_time
    logger.info(
        "\nDone. Sent %s events in %.1fs (%.1f/sec)",
        f"{count:,}",
        elapsed,
        count / elapsed,
    )


if __name__ == "__main__":
    main()
