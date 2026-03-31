from kafka import KafkaConsumer
from collections import defaultdict
from datetime import datetime
import json

consumer = KafkaConsumer(
    'lab1',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='velocity-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# STATE — this is what makes it stateful
# {user_id: [timestamp1, timestamp2, ...]}
user_timestamps = defaultdict(list)

WINDOW_SECONDS = 60
MAX_TX = 3

print("Monitoring for velocity anomalies...")

for message in consumer:
    tx = message.value
    user_id = tx['user_id']
    now = datetime.fromisoformat(tx['timestamp'])

    # Step 1: add current transaction timestamp to this user's history
    user_timestamps[user_id].append(now)

    # Step 2: remove timestamps older than 60 seconds (slide the window)
    user_timestamps[user_id] = [
        t for t in user_timestamps[user_id]
        if (now - t).total_seconds() <= WINDOW_SECONDS
    ]

    # Step 3: check if count exceeds limit
    count = len(user_timestamps[user_id])
    if count > MAX_TX:
        print(
            f"VELOCITY ALERT! user={user_id} | "
            f"{count} transactions in last 60s | "
            f"latest tx={tx['tx_id']} | amount={tx['amount']} PLN"
        )
