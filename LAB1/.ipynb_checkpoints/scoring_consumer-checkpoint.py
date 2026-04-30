from kafka import KafkaConsumer, KafkaProducer
import json

# Funkcja scoringowa wewnątrz pliku
def score_transaction(tx):
    score = 0
    rules = []
    if tx.get('amount', 0) > 3000:
        score += 3
        rules.append('R1')
    if tx.get('category') == 'elektronika' and tx.get('amount', 0) > 1500:
        score += 2
        rules.append('R2')
    if tx.get('hour', 24) < 6:
        score += 2
        rules.append('R3')
    return score, rules

consumer = KafkaConsumer(
    'transactions', 
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest', 
    group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

alert_producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("--- SCORING CONSUMER START ---")

for message in consumer:
    tx = message.value
    score, triggered_rules = score_transaction(tx)
    
    # Jeśli suma punktów >= 3, uznajemy transakcję za podejrzaną
    if score >= 3:
        # Dodajemy informacje o scoringu do wiadomości przed wysłaniem
        tx['fraud_score'] = score
        tx['triggered_rules'] = triggered_rules
        
        # Wysyłamy do tematu 'alerts'
        alert_producer.send('alerts', value=tx)
        alert_producer.flush()
        
        print(f"!!! FRAUD DETECTED !!! Score: {score} | ID: {tx['tx_id']} | Rules: {triggered_rules}")
    else:
        # Opcjonalnie: print(".", end="", flush=True) # Żeby widzieć, że żyje
        pass
