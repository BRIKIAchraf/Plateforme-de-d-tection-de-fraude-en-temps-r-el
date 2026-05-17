import time
import json
import random
from faker import Faker
from kafka import KafkaProducer
import os

# Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "transactions")
MSG_PER_SEC = int(os.getenv("MSG_PER_SEC", "1000"))

fake = Faker()

# Initialiser le producteur Kafka
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_transaction():
    is_fraud = random.random() < 0.02 # 2% de fraude

    if is_fraud:
        # Patterns de fraude
        amount = random.choice([1000.0, 5000.0, 10000.0]) # Montant rond élevé
        merchant = fake.company()
        country = fake.country_code() # Pays potentiellement inhabituel
        hour = random.randint(0, 5) # Nuit (00:00 - 05:00)
        timestamp = fake.date_time_between(start_date=f"-1d").replace(hour=hour).isoformat()
    else:
        # Transaction normale
        amount = round(random.uniform(5.0, 200.0), 2)
        merchant = fake.company()
        country = "FR" # Pays habituel
        hour = random.randint(6, 23)
        timestamp = fake.date_time_between(start_date=f"-1d").replace(hour=hour).isoformat()

    return {
        "transactionId": fake.uuid4(),
        "userId": fake.uuid4(),
        "cardId": fake.credit_card_number(),
        "amount": amount,
        "merchant": merchant,
        "country": country,
        "timestamp": timestamp,
        "is_fraud_simulated": is_fraud # Juste pour notre vérification, ne sera pas utilisé par le modèle en temps réel
    }

print(f"Démarrage du générateur de transactions vers {KAFKA_BROKER} (topic: {TOPIC_NAME})...")

try:
    while True:
        # Pour atteindre le débit souhaité (MSG_PER_SEC), on génère par batch
        batch_start_time = time.time()
        for _ in range(MSG_PER_SEC):
            tx = generate_transaction()
            producer.send(TOPIC_NAME, value=tx)
        
        producer.flush()
        
        elapsed_time = time.time() - batch_start_time
        sleep_time = max(0, 1.0 - elapsed_time)
        time.sleep(sleep_time)
        print(f"Envoyé {MSG_PER_SEC} messages.")
except KeyboardInterrupt:
    print("Arrêt du générateur.")
finally:
    producer.close()
