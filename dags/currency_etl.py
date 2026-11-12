from airflow.decorators import dag, task   # or airflow.sdk on Airflow 3
from datetime import datetime, timezone
import requests, pymongo

@dag(schedule="@daily", start_date=datetime(2026, 11, 1), catchup=False)
def currency_etl():

    @task
    def extract(ti=None):
        r = requests.get("https://api.frankfurter.dev/v2/rates",
                         params={"base": "EUR", "quotes": "USD,GBP,JPY"}, timeout=30)
        r.raise_for_status()
        ti.xcom_push(key="raw_rates", value=r.json())

    @task
    def transform(ti=None):
        raw = ti.xcom_pull(key="raw_rates", task_ids="extract")
        now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0).isoformat()
        docs = [{
            "base_currency": x["base"],
            "target_currency": x["quote"],
            "exchange_rate": x["rate"],
            "rate_date": x["date"],
            "source": "Frankfurter API",
            "retrieved_at": now,
        } for x in raw]            # adjust to the actual response shape
        ti.xcom_push(key="transformed_rates", value=docs)

    @task
    def load(ti=None):
        docs = ti.xcom_pull(key="transformed_rates", task_ids="transform")
        c = pymongo.MongoClient("mongodb://admin:admin123@mongo:27017/")
        c["currency_db"]["exchange_rates"].insert_many(docs)

    extract() >> transform() >> load()

currency_etl()