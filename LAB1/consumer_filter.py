from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Nasłuchuję na duże transakcje (amount > 3000)...")

for message in consumer:
    # Pobieramy dane transakcji ze słownika
    tx = message.value
    
    # if amount > 3000, jeśli tak — wypisz ALERT
    if tx['amount'] > 3000:
        print(f"ALERT: ID: {tx['tx_id']} | {tx['amount']:.2f} PLN | {tx['store']} | {tx['category']}")
