from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("--- KONSUMENT FILTRUJĄCY ---")

for message in consumer:
    # Pobieramy dane transakcji ze słownika
    tx = message.value
    
    # if amount > 1000, jeśli tak — wypisz ALERT
    if tx['amount'] > 1000:
        print(f"ALERT !!! Transakcja powyżej 1000: {tx['amount']:.2f} PLN | ID: {tx['tx_id']} | Kat: {tx['category']}")
