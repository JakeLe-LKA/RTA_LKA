from kafka import KafkaConsumer
import json

# YOUR CODE
# Read from 'transactions' (use a DIFFERENT group_id!)
# Add risk_level field based on amount
# Print enriched transaction
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='enriched-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Risk level:")

for message in consumer:
    tx = message.value
    if tx.get('amount', 0) > 1000:
        print(f"MEDIUM | {tx.get('tx_id')} | {tx.get('amount')}| {tx.get('store')} | {tx.get('category')}")
    elif tx.get('amount', 0) > 3000:
        print(f"HIGH | {tx.get('tx_id')} | {tx.get('amount')}| {tx.get('store')} | {tx.get('category')}")
    else:
        print(f"LOW | {tx.get('tx_id')} | {tx.get('amount')}| {tx.get('store')} | {tx.get('category')}")
