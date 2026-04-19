from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = {}
msg_count = 0

# YOUR CODE
# For each message:
#   1. Increment store_counts[store]
#   2. Add amount to total_amount[store]
#   3. Every 10 messages, print a summary table:
#      Store | Count | Total Amount | Avg Amount

for message in consumer:
    tx = message.value
    store = tx.get('store')
    amount = tx.get('amount', 0)

    store_counts[store] += 1

    total_amount[store] = total_amount.get(store, 0) + amount

    msg_count += 1

    if msg_count % 10 == 0:
        print(f"\n--- SUMMARY REPORT (Total messages: {msg_count}) ---")
        print(f"{'Store':<15} | {'Count':<8} | {'Total Amount':<15} | {'Avg Amount':<10}")
        print("-" * 60)
        
        for s in store_counts:
            count = store_counts[s]
            total = total_amount[s]
            avg = total / count
            print(f"{s:<15} | {count:<8} | {total:<12.2f} PLN | {avg:<10.2f} PLN")
        print("-" * 60)
