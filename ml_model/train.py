import os
import pandas as pd
import lightgbm as lgb
import mlflow
import mlflow.lightgbm
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, precision_score, recall_score
from imblearn.over_sampling import SMOTE

# Configuration MLflow
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
mlflow.set_experiment("Fraud_Detection_Experiment")

DATA_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'creditcard.csv')

def load_and_preprocess_data():
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"Le fichier {DATA_PATH} est introuvable. Veuillez le télécharger depuis Kaggle.")
    
    print("Chargement des données...")
    df = pd.read_csv(DATA_PATH)
    
    # Pour simuler nos features temps réel, on va mapper certaines colonnes existantes
    # car creditcard.csv contient V1...V28, Time, Amount, Class.
    # Dans la vraie vie, on utiliserait un dataset avec les vraies colonnes, 
    # mais pour correspondre à notre pipeline Flink, on va renommer/créer des features synthétiques
    # pour que l'API et le Modèle aient les mêmes noms de colonnes.
    
    print("Préparation des features pseudo-temps réel pour la compatibilité...")
    df['amount'] = df['Amount']
    df['count_5m'] = df['V1'].apply(lambda x: abs(x) * 10) # Synthétique
    df['sum_1h'] = df['amount'] * df['V2'].apply(lambda x: abs(x)) # Synthétique
    df['avg_24h'] = df['amount'] / (df['V3'].apply(lambda x: abs(x)) + 0.1) # Synthétique
    df['distinct_merchants'] = df['V4'].apply(lambda x: int(abs(x) * 5)) # Synthétique
    df['is_night'] = df['Time'].apply(lambda x: 1 if (x % 86400) / 3600 < 6 else 0)
    
    features = ['amount', 'count_5m', 'sum_1h', 'avg_24h', 'distinct_merchants', 'is_night']
    X = df[features]
    y = df['Class']
    
    return train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

def train_model():
    X_train, X_test, y_train, y_test = load_and_preprocess_data()
    
    print("Application de SMOTE pour gérer le déséquilibre...")
    smote = SMOTE(random_state=42)
    X_train_resampled, y_train_resampled = smote.fit_resample(X_train, y_train)
    
    print("Démarrage de l'entraînement avec MLflow...")
    with mlflow.start_run():
        params = {
            'objective': 'binary',
            'metric': 'auc',
            'boosting_type': 'gbdt',
            'learning_rate': 0.05,
            'num_leaves': 31,
            'max_depth': -1,
            'random_state': 42
        }
        
        mlflow.log_params(params)
        
        lgb_train = lgb.Dataset(X_train_resampled, y_train_resampled)
        lgb_eval = lgb.Dataset(X_test, y_test, reference=lgb_train)
        
        model = lgb.train(
            params,
            lgb_train,
            valid_sets=[lgb_train, lgb_eval],
            num_boost_round=100
        )
        
        print("Évaluation du modèle...")
        y_pred_prob = model.predict(X_test)
        y_pred = (y_pred_prob > 0.5).astype(int)
        
        auc = roc_auc_score(y_test, y_pred_prob)
        precision = precision_score(y_test, y_pred)
        recall = recall_score(y_test, y_pred)
        
        print(f"AUC: {auc:.4f}, Precision: {precision:.4f}, Recall: {recall:.4f}")
        
        mlflow.log_metric("auc", auc)
        mlflow.log_metric("precision", precision)
        mlflow.log_metric("recall", recall)
        
        # Log model and register it
        mlflow.lightgbm.log_model(
            lgb_model=model,
            artifact_path="model",
            registered_model_name="FraudDetectionModel"
        )
        
        print("Modèle entraîné et enregistré dans MLflow Registry sous le nom 'FraudDetectionModel'.")
        print("Pensez à taguer la version en 'Production' dans l'UI MLflow si ce n'est pas fait automatiquement.")

if __name__ == "__main__":
    train_model()
