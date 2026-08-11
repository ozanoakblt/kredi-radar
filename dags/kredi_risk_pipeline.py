"""
KrediRadar - Kredi Risk Skorlama Pipeline'ı (Airflow DAG)

Bu DAG, Seviye 1 notebook'undaki temel akışı (veri al -> doğrula ->
feature engineering -> model eğit -> MLflow'a logla -> başarı eşiği kontrolü)
otomatikleştirilmiş, zamanlanmış bir pipeline'a dönüştürür.

Not: EDA ve istatistiksel testler notebook'ta bir kez yapıldı, kararlar
(no_account kategorisi, log dönüşüm, RobustScaler, class_weight='balanced')
zaten verildi. Bu DAG, o kararları HER ÇALIŞTIĞINDA uygulayan production kodudur.
"""

from datetime import datetime
import pandas as pd
import numpy as np

from airflow import DAG
from airflow.operators.python import PythonOperator

DATA_PATH = "/opt/airflow/data/german_credit_data_updated.csv"
PROCESSED_DIR = "/opt/airflow/data/processed"
MLFLOW_TRACKING_URI = "sqlite:////opt/airflow/mlruns_shared/mlflow.db"
AUC_THRESHOLD = 0.70  # Bu eşiğin altında model registry'ye kaydedilmez


def ingest_data(**context):
    """Veriyi oku, ham haliyle bir sonraki adıma aktar."""
    df = pd.read_csv(DATA_PATH, index_col=0)
    import os
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    df.to_parquet(f"{PROCESSED_DIR}/raw.parquet")
    print(f"Veri okundu: {df.shape[0]} satır, {df.shape[1]} sütun")


def validate_data(**context):
    """Great Expectations'ın basitleştirilmiş hali: temel veri kalite kontrolleri.
    Gerçek bir üretim sisteminde burada Great Expectations kullanılır (İleri seviye)."""
    df = pd.read_parquet(f"{PROCESSED_DIR}/raw.parquet")

    assert df['Age'].min() >= 18, "HATA: 18 yaşından küçük başvuru var!"
    assert df['Credit amount'].min() > 0, "HATA: negatif/sıfır kredi tutarı var!"
    assert df['Duration'].min() > 0, "HATA: negatif/sıfır süre var!"
    assert 'Credit Risk' in df.columns, "HATA: hedef değişken eksik!"

    print("Veri kalite kontrolleri geçti.")


def preprocess_and_engineer_features(**context):
    """Notebook'ta Bölüm C ve D'de verilen kararları uygula:
    - Hedef değişkeni encode et
    - Eksik değerleri 'no_account' ile doldur (MNAR kararı)
    - Türetilmiş feature'ları oluştur (monthly_burden, age_group)
    - Train/test split (stratified)
    """
    from sklearn.model_selection import train_test_split

    df = pd.read_parquet(f"{PROCESSED_DIR}/raw.parquet")

    df['target'] = df['Credit Risk'].map({1: 0, 2: 1})
    df = df.drop(columns=['Credit Risk'])

    df['Saving accounts'] = df['Saving accounts'].fillna('no_account')
    df['Checking account'] = df['Checking account'].fillna('no_account')

    df['monthly_burden'] = df['Credit amount'] / df['Duration']
    df['age_group'] = pd.cut(df['Age'], bins=[0, 25, 40, 60, 100],
                              labels=['genc', 'orta_genc', 'orta_yasli', 'yasli'])

    X = df.drop(columns=['target'])
    y = df['target']

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    X_train.to_parquet(f"{PROCESSED_DIR}/X_train.parquet")
    X_test.to_parquet(f"{PROCESSED_DIR}/X_test.parquet")
    y_train.to_frame().to_parquet(f"{PROCESSED_DIR}/y_train.parquet")
    y_test.to_frame().to_parquet(f"{PROCESSED_DIR}/y_test.parquet")

    print(f"Preprocessing tamam. Train: {X_train.shape}, Test: {X_test.shape}")


def train_and_log_model(**context):
    """Notebook'ta bulunan en iyi konfigürasyonla (Random Forest,
    n_estimators=300, max_depth=5, min_samples_leaf=4) modeli eğit ve
    MLflow'a logla."""
    import mlflow
    import mlflow.sklearn
    from sklearn.compose import ColumnTransformer
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import RobustScaler, OneHotEncoder, FunctionTransformer
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import roc_auc_score

    X_train = pd.read_parquet(f"{PROCESSED_DIR}/X_train.parquet")
    X_test = pd.read_parquet(f"{PROCESSED_DIR}/X_test.parquet")
    y_train = pd.read_parquet(f"{PROCESSED_DIR}/y_train.parquet")['target']
    y_test = pd.read_parquet(f"{PROCESSED_DIR}/y_test.parquet")['target']

    numeric_cols = ['Age', 'Job', 'Credit amount', 'Duration', 'monthly_burden']
    categorical_cols = ['Sex', 'Housing', 'Saving accounts', 'Checking account', 'Purpose', 'age_group']

    skew_vals = X_train[numeric_cols].skew()
    skewed_numeric = skew_vals[skew_vals.abs() > 1].index.tolist()
    normal_numeric = [c for c in numeric_cols if c not in skewed_numeric]

    skewed_pipeline = Pipeline([
        ('log', FunctionTransformer(np.log1p, validate=False, feature_names_out='one-to-one')),
        ('scale', RobustScaler())
    ])

    preprocessor = ColumnTransformer(transformers=[
        ('skewed_num', skewed_pipeline, skewed_numeric),
        ('normal_num', RobustScaler(), normal_numeric),
        ('cat', OneHotEncoder(handle_unknown='ignore', drop='first'), categorical_cols)
    ])

    model = RandomForestClassifier(
        n_estimators=300, max_depth=5, min_samples_leaf=4,
        class_weight='balanced', random_state=42
    )

    pipe = Pipeline([('preprocess', preprocessor), ('clf', model)])

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment('kredi-radar-airflow')

    with mlflow.start_run(run_name=f"airflow_run_{context['ds']}"):
        pipe.fit(X_train, y_train)
        y_proba = pipe.predict_proba(X_test)[:, 1]
        test_auc = roc_auc_score(y_test, y_proba)

        mlflow.log_param('trigger', 'airflow_dag')
        mlflow.log_param('n_estimators', 300)
        mlflow.log_param('max_depth', 5)
        mlflow.log_metric('test_auc', test_auc)
        mlflow.sklearn.log_model(pipe, 'model', serialization_format='cloudpickle')

        run_id = mlflow.active_run().info.run_id

    print(f"Model eğitildi. Test AUC: {test_auc:.3f}. Run ID: {run_id}")

    # XCom ile bir sonraki task'a (threshold kontrolüne) aktar
    context['ti'].xcom_push(key='test_auc', value=test_auc)
    context['ti'].xcom_push(key='run_id', value=run_id)


def check_threshold_and_register(**context):
    """Test AUC belirlenen eşiğin üzerindeyse modeli registry'ye kaydet,
    değilse pipeline'ı BAŞARISIZ olarak işaretle (kötü model production'a sızmasın)."""
    import mlflow

    ti = context['ti']
    test_auc = ti.xcom_pull(key='test_auc', task_ids='train_and_log_model')
    run_id = ti.xcom_pull(key='run_id', task_ids='train_and_log_model')

    print(f"Test AUC: {test_auc:.3f}  |  Eşik: {AUC_THRESHOLD}")

    if test_auc < AUC_THRESHOLD:
        raise ValueError(
            f"Model kalite eşiğinin altında kaldı (AUC={test_auc:.3f} < {AUC_THRESHOLD}). "
            "Registry'ye kaydedilmedi, pipeline durduruldu."
        )

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = mlflow.tracking.MlflowClient()
    model_uri = f"runs:/{run_id}/model"
    registered = mlflow.register_model(model_uri, "kredi-radar-model")

    print(f"Eşik geçildi, model registry'ye kaydedildi: versiyon {registered.version}")


default_args = {
    "owner": "ozan",
    "retries": 1,
}

with DAG(
    dag_id="kredi_risk_pipeline",
    description="KrediRadar - kredi risk skorlama uçtan uca pipeline",
    default_args=default_args,
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["kredi-radar", "mlops", "seviye2"],
) as dag:

    t1 = PythonOperator(task_id="ingest_data", python_callable=ingest_data)
    t2 = PythonOperator(task_id="validate_data", python_callable=validate_data)
    t3 = PythonOperator(task_id="preprocess_and_engineer_features", python_callable=preprocess_and_engineer_features)
    t4 = PythonOperator(task_id="train_and_log_model", python_callable=train_and_log_model)
    t5 = PythonOperator(task_id="check_threshold_and_register", python_callable=check_threshold_and_register)

    t1 >> t2 >> t3 >> t4 >> t5
