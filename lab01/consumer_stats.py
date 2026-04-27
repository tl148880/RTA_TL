from kafka import KafkaConsumer
from collections import defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='consumer-stats-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

stats = defaultdict(lambda: {
    'count': 0,
    'total': 0,
    'min': float('inf'),
    'max': float('-inf')
})

msg_count = 0

for message in consumer:
    event = message.value

    category = event.get('category', 'UNKNOWN')
    amount = event.get('amount', 0)

    stats[category]['count'] += 1
    stats[category]['total'] += amount
    stats[category]['min'] = min(stats[category]['min'], amount)
    stats[category]['max'] = max(stats[category]['max'], amount)

    msg_count += 1

    if msg_count % 10 == 0:
        print("\n--- STATYSTYKI PER KATEGORIA ---")
        print(f"{'Kategoria':<15} {'Liczba':<10} {'Suma':<12} {'Min':<10} {'Max':<10}")

        for category, data in stats.items():
            print(
                f"{category:<15} "
                f"{data['count']:<10} "
                f"{data['total']:<12.2f} "
                f"{data['min']:<10.2f} "
                f"{data['max']:<10.2f}"
            )
