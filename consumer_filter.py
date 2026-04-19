from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='filter-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Listening for large transactions (amount > 1000)...")

# YOUR CODE
# For each message: check if amount > 1000, if so print an alert
# Format: ALERT: TX0042 | 2345.67 PLN | Warsaw | electronics

for message in consumer:
    tx = message.value
    if tx.get('amount', 0) > 1000:
        print(f"ALERT: {tx.get('tx_id')} | {tx.get('amount')}| {tx.get('store')} | {tx.get('category')}")
