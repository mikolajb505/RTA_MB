from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Konsument analizujący ryzyko")

for message in consumer:
    tx = message.value
    amount = tx['amount']
    
    # przypisywanie poziomu ryzyka
    if amount > 3000:
        risk_level = "HIGH"
    elif amount > 1000:
        risk_level = "MEDIUM"
    else:
        risk_level = "LOW"
    
    # nowe pole do słownika
    tx['risk_level'] = risk_level
    
    # Wyświetlamy wynik z nowym polem
    print(f"[{tx['risk_level']:6}] | ID: {tx['tx_id']} | {tx['amount']:.2f} PLN | {tx['store']} | {tx['category']}")
