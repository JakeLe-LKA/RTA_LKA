from kafka import KafkaConsumer
from collections import defaultdict
import json

# YOUR CODE
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='stats-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

stats = {}
msg_count = 0

print("Stats:")

for message in consumer:
    tx = message.value
    cat = tx.get('category')
    amount = tx.get('amount', 0)

    if cat not in stats:
        stats[cat] = {
            'count': 0, 
            'total': 0, 
            'min': float('inf'), 
            'max': float('-inf')
        }

    stats[cat]['count'] += 1
    stats[cat]['total'] += amount
    if amount < stats[cat]['min']: stats[cat]['min'] = amount
    if amount > stats[cat]['max']: stats[cat]['max'] = amount
    
    msg_count += 1

    if msg_count % 10 == 0:
        print(f"\n{'='*85}")
        print(f"{'Category':<15} | {'Count':<6} | {'Min':<10} | {'Max':<10} | {'Avg':<10} | {'Total':<12}")
        print(f"{'-'*85}")
        
        for cat_name, data in stats.items():
            avg = data['total'] / data['count']
            print(f"{cat_name:<15} | {data['count']:<6} | {data['min']:<10.2f} | {data['max']:<10.2f} | {avg:<10.2f} | {data['total']:<12.2f}")
        print(f"{'='*85}")
