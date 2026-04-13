from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='consumer-count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = {}
msg_count = 0

for message in consumer:
    event = message.value

    store = event.get('store', 'UNKNOWN')
    amount = event.get('amount', 0)

    store_counts[store] += 1

    total_amount[store] = total_amount.get(store, 0) + amount

    msg_count += 1

    if msg_count % 10 == 0:
        print("\n--- PODSUMOWANIE ---")
        print(f"{'Sklep':<15} {'Liczba':<10} {'Suma':<10} {'Średnia':<10}")

        for store in store_counts:
            count = store_counts[store]
            total = total_amount[store]
            avg = total / count if count > 0 else 0

            print(f"{store:<15} {count:<10} {total:<10.2f} {avg:<10.2f}")
