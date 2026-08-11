# KrediRadar — Seviye 1

Finans domaininde MLOps + RAG portföy projesi — Seviye 1: EDA + İstatistiksel Testler +
Preprocessing + Modelleme + MLflow.

## Kurulum

```bash
pip install -r requirements.txt
```

## Çalıştırma

1. `german_credit_data_updated.csv` dosyasını bu klasöre koy (zaten burada).
2. Jupyter'ı başlat: `jupyter notebook`
3. `KrediRadar_seviye1_tam.ipynb` dosyasını aç, hücreleri sırayla çalıştır.
4. MLflow arayüzünü görmek için: `mlflow ui --backend-store-uri sqlite:///mlflow.db`

## Veri Seti

German Credit Data (Statlog, UCI) — CC BY 4.0 lisanslı.
Kaynak: https://archive.ics.uci.edu/dataset/144/statlog+german+credit+data

## Sıradaki Adım

Bu notebook'un ilgili kısımları `.py` script'lerine bölünüp Airflow DAG'ına taşınacak (Seviye 2).
