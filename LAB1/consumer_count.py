from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = {}
msg_count = 0

print("Konsument statystyczny")

for message in consumer:
    tx = message.value
    store = tx['store']
    amount = tx['amount']
    
    # 1. Zwiększamy licznik transakcji dla sklepu
    store_counts[store] += 1
    
    # 2. Dodajemy kwotę do sumy (inicjalizacja klucza jeśli nie istnieje)
    if store not in total_amount:
        total_amount[store] = 0.0
    total_amount[store] += amount
    
    msg_count += 1
    
    # 3. Co 10 wiadomości tabela podsumowująca
    if msg_count % 10 == 0:
        print(f"\n[ Raport po {msg_count} wiadomościach ]")
        print(f"{'Sklep':<12} | {'Liczba':<7} | {'Suma':<12} | {'Średnia':<10}")
        print("-" * 50)
        
        for s in sorted(store_counts.keys()):
            count = store_counts[s]
            total = total_amount[s]
            avg = total / count
            print(f"{s:<12} | {count:<7} | {total:>9.2f} zł | {avg:>8.2f} zł")
        print("-" * 50)
