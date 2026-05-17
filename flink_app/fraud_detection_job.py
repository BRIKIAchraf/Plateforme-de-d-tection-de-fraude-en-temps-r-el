import os
import json
import time
import requests
import redis
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
FASTAPI_URL = os.getenv("FASTAPI_URL", "http://fastapi:8000/score")

class FeatureEngineeringAndScoring(MapFunction):
    def __init__(self):
        self.redis_client = None
        
    def open(self, runtime_context):
        # Initialiser Redis
        self.redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
        
    def map(self, value):
        try:
            tx = json.loads(value)
            card_id = tx.get("cardId")
            user_id = tx.get("userId")
            amount = tx.get("amount", 0.0)
            merchant = tx.get("merchant")
            
            card_key = f"features:card:{card_id}"
            user_key = f"features:user:{user_id}"
            merchant_set_key = f"features:merchants:{card_id}"
            
            # Feature 1: nb transactions par carte - 5 min (TTL)
            self.redis_client.hincrby(card_key, "count_5m", 1)
            # Feature 2: montant total - 1h
            self.redis_client.hincrbyfloat(card_key, "sum_1h", amount)
            # Feature 3: montant moyen par user - 24h
            self.redis_client.hincrbyfloat(user_key, "sum_24h", amount)
            self.redis_client.hincrby(user_key, "count_24h", 1)
            user_sum = float(self.redis_client.hget(user_key, "sum_24h") or 0.0)
            user_count = int(self.redis_client.hget(user_key, "count_24h") or 1)
            self.redis_client.hset(user_key, "avg_24h", user_sum / user_count)
            # Feature 4: marchands distincts - 15 min
            self.redis_client.sadd(merchant_set_key, merchant)
            distinct_merchants = self.redis_client.scard(merchant_set_key)
            self.redis_client.hset(card_key, "distinct_merchants_15m", distinct_merchants)
            
            # Set TTLs to approximate windows
            self.redis_client.expire(card_key, 3600) # 1h max
            self.redis_client.expire(user_key, 86400) # 24h max
            self.redis_client.expire(merchant_set_key, 900) # 15m max

            # Appel API Scoring
            start_time = time.time()
            response = requests.post(FASTAPI_URL, json=tx, timeout=2)
            score_data = response.json() if response.status_code == 200 else {}
            latency_ms = (time.time() - start_time) * 1000

            score = score_data.get("score", 0.0)
            decision = score_data.get("decision", "ERROR")

            result = {
                "transaction": tx,
                "score": score,
                "decision": decision,
                "latency_ms": latency_ms
            }

            # On retourne en JSON pour Kafka, on peut filtrer après si on veut
            return json.dumps(result)
        except Exception as e:
            return json.dumps({"error": str(e), "transaction_raw": value})

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    # Ajout du connecteur Kafka
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-1.17.1.jar")
    
    kafka_consumer = FlinkKafkaConsumer(
        topics='transactions',
        deserialization_schema=SimpleStringSchema(),
        properties={'bootstrap.servers': KAFKA_BROKER, 'group.id': 'flink_fraud_group'}
    )
    kafka_consumer.set_start_from_latest()

    ds = env.add_source(kafka_consumer)
    
    # Transformation et Scoring
    scored_ds = ds.map(FeatureEngineeringAndScoring(), output_type=Types.STRING())
    
    # Filtre pour ne garder que les fraudes ou tout garder
    # Ici, on garde tout dans `fraud-scores` pour le monitoring et l'audit
    
    kafka_producer = FlinkKafkaProducer(
        topic='fraud-scores',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': KAFKA_BROKER}
    )
    
    scored_ds.add_sink(kafka_producer)
    
    env.execute("Fraud Detection PyFlink Job")

if __name__ == '__main__':
    main()
