from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Inicjalizacja statystyk
category_counts = Counter()
category_total_amount = {}
category_min = {}
category_max = {}
msg_count = 0

print("Konsument statystyczny per produkt")

for message in consumer:
    tx = message.value
    category = tx['category']
    amount = tx['amount']
    
    # 1. Zliczanie
    category_counts[category] += 1
    
    # 2. Inicjalizacja i Sumowanie
    if category not in category_total_amount:
        category_total_amount[category] = 0.0
        category_min[category] = amount
        category_max[category] = amount
    
    category_total_amount[category] += amount
    
    # 3. Aktualizacja MIN i MAX
    if amount < category_min[category]:
        category_min[category] = amount
    if amount > category_max[category]:
        category_max[category] = amount
    
    msg_count += 1
    
    # 4. Tabela co 10 wiadomości
    if msg_count % 10 == 0:
        print(f"\n[ Raport po {msg_count} wiadomościach ]")
        # Nagłówki z nową szerokością:
        # Kategoria (12), Liczba (7), Łączny przychód (18), Min (12), Max (12)
        print(f"{'Kategoria':<12} | {'Liczba':<7} | {'Łączny przychód':<18} | {'Min':<12} | {'Max':<12}")
        print("-" * 75)
        
        for cat in sorted(category_counts.keys()):
            count = category_counts[cat]
            total = category_total_amount[cat]
            c_min = category_min[cat]
            c_max = category_max[cat]
            
            # Formatowanie danych:
            # :>15.2f daje nam wyrównanie kwoty do prawej wewnątrz kolumny 18-znakowej (zostawiając margines na ' zł')
            print(f"{cat:<12} | {count:<7} | {total:>15.2f} zł | {c_min:>9.2f} zł | {c_max:>9.2f} zł")
        print("-" * 75)
