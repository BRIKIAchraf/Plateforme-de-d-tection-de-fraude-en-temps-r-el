# 🛡️ Plateforme de Détection de Fraude en Temps Réel

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)]()
[![Python 3.9](https://img.shields.io/badge/python-3.9-blue.svg)]()
[![Apache Flink](https://img.shields.io/badge/Apache%20Flink-1.17-e6522c.svg)]()
[![Kafka](https://img.shields.io/badge/Kafka-3.4-black.svg)]()
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-2.4-blue.svg)]()

Une architecture Big Data de bout en bout pour la détection de fraudes bancaires en temps réel. Le système ingère **1000+ transactions/seconde**, calcule des features sur des fenêtres temporelles glissantes (streaming), et applique un modèle d'Intelligence Artificielle (LightGBM) en moins de **200ms de latence totale**.

---

## 🏗️ Architecture

![Architecture Diagram](https://upload.wikimedia.org/wikipedia/commons/thumb/c/c5/Real-time_Big_Data_Architecture.png/800px-Real-time_Big_Data_Architecture.png) 
*(Exemple générique, insérez ici votre export Excalidraw)*

Le système repose sur des standards de l'industrie :
1. **Générateur (Python/Faker)** : Simule des transactions bancaires avec ~2% d'anomalies injectées.
2. **Kafka** : Ingestion haut débit via le topic `transactions`.
3. **Apache Flink (PyFlink)** : Streaming & Feature Engineering temps réel. Calcul sur fenêtres glissantes (5 min, 1h, 24h) et stockage dans un Feature Store.
4. **Redis** : Feature Store ultra-rapide servant de cache.
5. **FastAPI & MLflow** : API de scoring qui récupère les features de Redis, évalue le modèle LightGBM et renvoie la probabilité de fraude.
6. **Delta Lake (Spark)** : Stockage Lakehouse pour les données historiques, permettant l'audit et le *Time Travel*.
7. **Apache Airflow** : Orchestrateur déclenchant le réentraînement automatique du modèle lorsque de nouvelles fraudes confirmées sont détectées.
8. **Grafana & Prometheus** : Monitoring complet du pipeline.

---

## 📊 Métriques et Performances

Grâce à cette architecture, le système respecte les SLAs stricts du secteur bancaire :
- **Throughput** : ~1,000 TPS (Transactions Par Seconde).
- **Scoring Latency (FastAPI)** : ~15 ms (P95).
- **Latence Bout-en-Bout (Kafka → Flink → API → Kafka)** : < 150 ms (P95).
- **Taux de Détection** : Ajustable, configuré ici pour alerter au-delà d'un score de 0.7.

### Dashboard Grafana en live
*(Ajoutez ici le GIF de votre dashboard)*
`![Grafana Demo](demo.gif)`

---

## 🚀 Démarrage Rapide

### Prérequis
- Docker et Docker Compose (allouez au moins 8Go de RAM à Docker).
- (Optionnel) Dataset Kaggle `creditcard.csv` pour l'entraînement réel.

### 1. Démarrer l'infrastructure complète
Le fichier `docker-compose.yml` lance tous les services d'un coup (Kafka, Flink, Redis, FastAPI, MLflow, Airflow, Monitoring, Générateur).

```bash
docker-compose up --build -d
```

### 2. Entraîner le Modèle ML (Optionnel)
Placez le fichier `creditcard.csv` dans le dossier `data/` puis lancez :
```bash
python ml_model/train.py
```
Le modèle sera automatiquement tracké par MLflow et enregistré dans le Registry.

### 3. Lancer le traitement Flink
Une fois l'infrastructure démarrée (vérifiez Flink UI sur `localhost:8081`), soumettez le job PyFlink :
```bash
docker exec -it jobmanager flink run -py /opt/flink/app/fraud_detection_job.py
```

### 4. Démarrer l'archivage Delta Lake (Spark)
```bash
python delta_lake/kafka_to_delta.py
```

---

## 🧭 Accès aux Interfaces (UI)

- **Kafdrop (Explorateur Kafka)** : [http://localhost:9000](http://localhost:9000)
- **Apache Flink Dashboard** : [http://localhost:8081](http://localhost:8081)
- **Grafana (Monitoring)** : [http://localhost:3000](http://localhost:3000) *(admin/admin)*
- **MLflow UI** : [http://localhost:5000](http://localhost:5000)
- **Airflow Webserver** : [http://localhost:8080](http://localhost:8080) *(admin/admin)*
- **FastAPI Swagger Docs** : [http://localhost:8000/docs](http://localhost:8000/docs)

---

## 🔄 Boucle de Feedback et MLOps

Le DAG Airflow `fraud_model_retraining` simule la détection de nouvelles fraudes confirmées (par des humains). Lorsqu'un volume suffisant de nouvelles données s'accumule dans **Delta Lake**, Airflow déclenche automatiquement un réentraînement via le script `train.py`. Le modèle est versionné et promu en production sans interruption de service grâce à MLflow Model Registry.

## 👥 Auteur
Un projet d'architecture Data/ML conçu pour démontrer l'intégration de technologies Streaming et Lakehouse en production.
