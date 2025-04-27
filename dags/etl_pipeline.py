from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {"owner": "data_eng", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="daily_transactions_etl",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
) as dag:
    ingest = SparkSubmitOperator(
        task_id="ingest_raw",
        application="spark_jobs/ingest_raw.py",
        conn_id="spark_default",
    )
    transform = SparkSubmitOperator(
        task_id="transform_curated",
        application="spark_jobs/transform_curated.py",
        conn_id="spark_default",
    )
    ingest >> transform
