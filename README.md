# KrediRadar 💳

Finans domaininde uçtan uca bir MLOps portföy projesi — kredi risk skorlaması, **MLflow** ile deney takibi ve model governance, **Apache Airflow** ile otomatik/zamanlanmış pipeline orkestrasyonu üzerine kurulu.

## Proje Amacı

Bankaların kredi başvurularını değerlendirirken kullandığı türde bir modelin, **production disipliniyle** (deney takibi, otomatik pipeline, kalite eşiği kontrolü, model registry) nasıl kurulacağını göstermek.

## Veri Seti

German Credit Data (Statlog) — UCI Machine Learning Repository, CC BY 4.0 lisanslı. 954 kayıt, kredi başvurularının "iyi risk / kötü risk" olarak sınıflandırılması.

## Mimari

Veri (CSV) → Airflow DAG → MLflow (deney takibi + model registry)
ingest → validate → preprocess/feature engineering → train → threshold kontrolü

- **Seviye 1 — Model Geliştirme:** Kapsamlı bir Jupyter notebook (EDA, istatistiksel testler, preprocessing, feature engineering, 3 model karşılaştırması, hiperparametre optimizasyonu, MLflow entegrasyonu, model değerlendirme)
- **Seviye 2 — Orkestrasyon:** Notebook'taki kararları otomatikleştiren bir Airflow DAG'ı (Docker Compose ile LocalExecutor + Postgres)

## Kullanılan Teknolojiler

| Kategori | Araç |
|---|---|
| Deney takibi & Model Registry | MLflow |
| Orkestrasyon | Apache Airflow (LocalExecutor) |
| Konteynerleştirme | Docker, Docker Compose |
| Modelleme | scikit-learn, XGBoost |
| İstatistik | SciPy, statsmodels |
| Açıklanabilirlik | SHAP |

## Öne Çıkan Teknik Kararlar

- **Eksik değer yönetimi:** Saving/Checking account sütunlarındaki eksik değerler MNAR olarak değerlendirildi — "no_account" kategorisi kullanıldı.
- **Model seçimi:** Logistic Regression, Random Forest ve XGBoost karşılaştırıldı; optimize edilen Random Forest en iyi sonucu verdi (CV AUC: 0.756 → 0.771).
- **Maliyet-duyarlı değerlendirme:** Resmi 5:1 maliyet matrisi kullanılarak optimal karar eşiği belirlendi.
- **Kalite kapısı:** Airflow DAG'ı, test AUC'si 0.70 eşiğinin altında kalan modeli reddedip Model Registry'ye kaydetmiyor.

## Ekran Görüntüleri

### MLflow — Deney Karşılaştırması
![MLflow runs](screenshots/mlflow_01.jpeg)

### MLflow — Model Parametreleri
![MLflow parameters](screenshots/mlflow_02.png)

### MLflow — Model Registry
![MLflow model registry](screenshots/mlflow_05.png)

### Airflow — DAG Çalıştırması (5 Task Başarılı)
![Airflow DAG success](screenshots/airflow_01.png)

### Airflow ve MLflow Entegrasyonu
![Airflow MLflow integration](screenshots/airflow_sonrası_mlflow.png)

## Kurulum ve Çalıştırma

### Seviye 1 (Notebook)

    pip install -r requirements.txt
    jupyter notebook KrediRadar_seviye1_tam.ipynb

### Seviye 2 (Airflow Pipeline)

    docker-compose up airflow-init
    docker-compose up

Airflow arayüzü: http://localhost:8081 (admin/admin)

## Yol Haritasının Devamı

Bu proje, daha geniş bir finans MLOps + RAG portföyünün parçası. Planlanan sonraki adımlar: model serving (FastAPI), monitoring (Prometheus + Grafana), CI/CD (GitHub Actions).

## Lisans / Veri Kaynağı

Veri seti CC BY 4.0 lisanslıdır. Bu proje eğitim/portföy amaçlıdır.


