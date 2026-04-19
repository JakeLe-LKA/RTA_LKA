from kafka import KafkaConsumer
import json
from datetime import datetime, timedelta

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='velocity-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_history = {}

print("Velocity Detection...")

for message in consumer:
    tx = message.value
    user_id = tx.get('user_id')
    
    current_time = datetime.fromisoformat(tx.get('timestamp'))
    
    if user_id not in user_history:
        user_history[user_id] = []
    
    user_history[user_id].append(current_time)
    
    sixty_seconds_ago = current_time - timedelta(seconds=60)
    user_history[user_id] = [t for t in user_history[user_id] if t > sixty_seconds_ago]
    
    if len(user_history[user_id]) > 3:
        print(f"⚠️  VELOCITY ALERT: User {user_id} made {len(user_history[user_id])} transactions in 60 seconds!")
        print(f"   Detailed: {tx['tx_id']} | {tx['amount']} PLN | {tx['store']}")
        print("-" * 50)
