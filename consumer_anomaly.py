
from kafka import KafkaConsumer
from collections import defaultdict, deque
import json
import time
from datetime import datetime

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='consumer-anomaly-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_events = defaultdict(deque)

window = 60

for message in consumer:
    event = message.value
    user_id = event.get('user_id', 'unknown')
    raw_ts = event.get('timestamp')

    try:
        timestamp = float(raw_ts)
    except (TypeError, ValueError):
        try:
            timestamp = datetime.fromisoformat(raw_ts).timestamp()
        except Exception:
            timestamp = time.time()

    user_events[user_id].append(timestamp)

    while user_events[user_id] and timestamp - user_events[user_id][0] > window:
        user_events[user_id].popleft()

    if len(user_events[user_id]) > 3:
        print(f"ALERT: user {user_id} wykonał > 3 transakcje w 60s")
        print(f"Zdarzenia: {list(user_events[user_id])}")
