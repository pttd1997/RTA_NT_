import json
import random
import time
import uuid
from datetime import datetime

from kafka import KafkaProducer


# ==================================================
# CONFIG
# ==================================================

BOOTSTRAP_SERVERS = ["broker:9092"]
TOPIC = "transactions"

# ==================================================
# REFERENCE DATA
# ==================================================

CUSTOMERS = [f"U{i:05d}" for i in range(1, 5001)]

MERCHANT_CATEGORIES = [
    "groceries",
    "electronics",
    "fashion",
    "travel",
    "gaming",
    "restaurant",
    "fuel",
    "pharmacy"
]

PAYMENT_METHODS = [
    "card",
    "blik",
    "paypal",
    "bank_transfer"
]

COUNTRIES = [
    "Poland",
    "Germany",
    "France",
    "Spain",
    "Netherlands",
    "Vietnam",
    "Nigeria",
    "Brazil"
]

# ==================================================
# KAFKA PRODUCER
# ==================================================

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ==================================================
# GENERATORS
# ==================================================

def generate_amount():
    """
    Realistic transaction distribution:
    80% small
    15% medium
    5% large
    """

    r = random.random()

    if r < 0.80:
        return round(random.uniform(10, 500), 2)

    elif r < 0.95:
        return round(random.uniform(500, 3000), 2)

    else:
        return round(random.uniform(3000, 10000), 2)


def generate_device():

    return random.choices(
        ["known_device", "new_device"],
        weights=[90, 10]
    )[0]


def generate_country():

    return random.choices(
        COUNTRIES,
        weights=[
            85,  # Poland
            4,   # Germany
            3,   # France
            3,   # Spain
            2,   # Netherlands
            1,   # Vietnam
            1,   # Nigeria
            1    # Brazil
        ]
    )[0]


def generate_transaction():

    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": random.choice(CUSTOMERS),
        "merchant_id": f"M{random.randint(1,1000):04d}",
        "amount": generate_amount(),
        "currency": "PLN",
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "payment_method": random.choice(PAYMENT_METHODS),
        "device": generate_device(),
        "user_country": "Poland",
        "transaction_country": generate_country(),
        "timestamp": datetime.utcnow().isoformat()
    }

# ==================================================
# MAIN LOOP
# ==================================================

print("=" * 60)
print("REAL-TIME TRANSACTION PRODUCER STARTED")
print("=" * 60)
print(f"Kafka Topic: {TOPIC}")
print()

counter = 0

try:

    while True:

        counter += 1

        transaction = generate_transaction()

        producer.send(
            TOPIC,
            value=transaction
        )

        if counter % 10 == 0:

            print(
                f"Sent {counter} transactions | "
                f"Amount={transaction['amount']} PLN | "
                f"Country={transaction['transaction_country']} | "
                f"Device={transaction['device']}"
            )

        # simulate real-time stream
        time.sleep(random.uniform(0.2, 1.0))

except KeyboardInterrupt:

    print("\nStopping producer...")

finally:

    producer.flush()
    producer.close()

    print("Producer closed.")