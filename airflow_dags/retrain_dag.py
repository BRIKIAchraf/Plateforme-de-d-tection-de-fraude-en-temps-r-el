from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fraud_model_retraining',
    default_args=default_args,
    description='Retrain Fraud Detection Model when new data arrives',
    schedule_interval=timedelta(days=1), # Retrain tous les jours
    catchup=False
)

def check_new_data():
    """
    Simulation d'une vérification dans Delta Lake pour de nouvelles fraudes confirmées.
    Dans un vrai scénario, on ferait:
    df = spark.read.format("delta").load(DELTA_PATH)
    if df.count() > SEUIL: return True
    """
    print("Vérification des nouvelles données dans Delta Lake...")
    # On simule qu'il y a de nouvelles données
    return True

check_data_task = PythonOperator(
    task_id='check_new_data_delta',
    python_callable=check_new_data,
    dag=dag,
)

# On suppose que le script train.py est monté dans /opt/airflow/dags ou qu'on utilise un container
# Ici, pour la simplicité, on simule l'appel (on peut aussi configurer DockerOperator)
retrain_model_task = BashOperator(
    task_id='retrain_model_mlflow',
    # Dans une vraie prod Airflow sur Docker, on utiliserait DockerOperator pour lancer ml_model/train.py
    # ou on s'assurerait que l'environnement Airflow a les librairies ML.
    bash_command='echo "Simulation de réentraînement LightGBM et enregistrement dans MLflow..."',
    dag=dag,
)

check_data_task >> retrain_model_task
