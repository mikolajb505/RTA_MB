from kafka import KafkaConsumer
import json
from collections import defaultdict
from datetime import datetime

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Klucz: user_id, wartość: lista krotek (timestamp, amount)
user_history = defaultdict(list)

print("Detektor anamalii prędkości")

for message in consumer:
    tx = message.value
    user_id = tx['user_id']
    amount = tx['amount']
    current_time = datetime.fromisoformat(tx['timestamp'])
    
    # 1. Dodajemy parę (czas, kwota) do historii
    user_history[user_id].append((current_time, amount))
    
    # 2. Czyścimy starą historię
    user_history[user_id] = [
        t for t in user_history[user_id] 
        if (current_time - t[0]).total_seconds() <= 60
    ]
    
    # 3. Sprawdzamy warunek prędkości
    if len(user_history[user_id]) > 3:
        # Sumujemy drugie elementy (kwoty) z przefiltrowanej listy
        total_velocity_amount = sum(t[1] for t in user_history[user_id])
        
        print(f"ALERT !!! Użytkownik {user_id} wykonał "
              f"{len(user_history[user_id])} transakcje w ciągu 60s! "
              f"Łączna kwota: {total_velocity_amount:.2f} PLN")
