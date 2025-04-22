from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from google.cloud import storage
import pandas as pd
from io import StringIO


def initial_task():
    print("DAG is working...")

def list_bucket_files():
    bucket_name = 'etl_sap'
    prefix = ''

    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    for blob in blobs:
        if blob.name.endswith('.csv'):
            content = blob.download_as_text()
            df = pd.read_csv(StringIO(content))

with DAG(
    dag_id = 'pipeline_etl_sap_gcp',
    start_date=datetime(2024, 1, 1),
    schedule_interval = None,
    catchup = False,
    tags = ['sap', 'etl', 'gcp']
) as dag:

    task_inicial = PythonOperator(
        task_id = 'initial_task',
        python_callable = initial_task
    )

    task_listar_arquivos = PythonOperator(
        task_id = 'list_bucket_files',
        python_callable = list_bucket_files
    )

    task_inicial >> task_listar_arquivos