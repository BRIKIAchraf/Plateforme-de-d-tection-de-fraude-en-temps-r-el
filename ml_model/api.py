import os
import json
import time
import pandas as pd
import redis
import mlflow.pyfunc
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from prometheus_client import make_asgi_app, Counter, Histogram

# Configuration
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
MODEL_NAME = "FraudDetectionModel"
MODEL_STAGE = "Production"

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

app = FastAPI(title="Fraud Scoring API")

# Metrics Prometheus
SCORING_LATENCY = Histogram('scoring_latency_seconds', 'Latency of scoring requests')
FRAUD_DETECTED = Counter('fraud_detected_total', 'Total number of detected frauds')
REQUESTS_TOTAL = Counter('scoring_requests_total', 'Total number of scoring requests')

# Add prometheus asgi middleware
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# Redis connection
redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)

# Load model lazily
model = None

def get_model():
    global model
    if model is None:
        try:
            model_uri = f"models:/{MODEL_NAME}/{MODEL_STAGE}"
            model = mlflow.pyfunc.load_model(model_uri)
            print(f"Model {MODEL_NAME} loaded successfully from MLflow.")
        except Exception as e:
            print(f"Model not found or error loading model: {e}")
    return model

class TransactionInput(BaseModel):
    transactionId: str
    userId: str
    cardId: str
    amount: float
    merchant: str
    country: str
    timestamp: str

@app.post("/score")
@SCORING_LATENCY.time()
def score_transaction(tx: TransactionInput):
    REQUESTS_TOTAL.inc()
    
    # 1. Fetch features from Redis
    # Features attendues:
    # - count_5m: nb transactions par carte dans les 5 min
    # - sum_1h: montant total par carte dans l'heure
    # - avg_24h: montant moyen par userId dans 24h
    # - distinct_merchants_15m: nb marchands distincts par carte dans 15 min
    # - is_night: heure nocturne (22h–6h) -> calculé ici ou dans Flink

    card_key = f"features:card:{tx.cardId}"
    user_key = f"features:user:{tx.userId}"

    count_5m = float(redis_client.hget(card_key, "count_5m") or 0.0)
    sum_1h = float(redis_client.hget(card_key, "sum_1h") or 0.0)
    avg_24h = float(redis_client.hget(user_key, "avg_24h") or 0.0)
    distinct_merchants = float(redis_client.hget(card_key, "distinct_merchants_15m") or 0.0)
    
    hour = pd.to_datetime(tx.timestamp).hour
    is_night = 1 if (hour >= 22 or hour <= 6) else 0

    # Préparation du DataFrame pour LightGBM
    # Les features doivent correspondre exactement à ce avec quoi le modèle a été entraîné
    features_df = pd.DataFrame([{
        "amount": tx.amount,
        "count_5m": count_5m,
        "sum_1h": sum_1h,
        "avg_24h": avg_24h,
        "distinct_merchants": distinct_merchants,
        "is_night": is_night
    }])

    # 2. Score with MLflow model
    current_model = get_model()
    if not current_model:
        # Fallback dummy logic if model not yet trained
        score = 0.9 if (tx.amount > 1000 and is_night == 1) else 0.01
    else:
        score = float(current_model.predict(features_df)[0])

    if score > 0.7:
        FRAUD_DETECTED.inc()

    return {
        "transactionId": tx.transactionId,
        "score": score,
        "decision": "FRAUD" if score > 0.7 else "NORMAL"
    }

@app.get("/health")
def health_check():
    return {"status": "healthy"}
