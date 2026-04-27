from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='consumer-enrich-group',  # inny group_id
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    event = message.value

    amount = event.get('amount', 0)

    if amount > 3000:
        event['risk_level'] = "HIGH"
    elif amount > 1000:
        event['risk_level'] = "MEDIUM"
    else:
        event['risk_level'] = "LOW"

    print(f"{event}")
