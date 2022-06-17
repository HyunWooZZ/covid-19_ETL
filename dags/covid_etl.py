from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from config.config import project_id, staging_dataset, dwh_dataset, gs_bucket

from datetime import datetime, timedelta

default_args = {
    'owner': 'HyunWoo Oh',
    'start_date': datetime(2020, 3, 1),
    'retries' : 3,
    'retry_delay' : timedelta(minutes=5),
    'email_on_retry' : False,
    'email_on_failure' : False
}

# Define dag variables
project_id = project_id
staging_dataset = staging_dataset
dwh_dataset = dwh_dataset
gs_bucket = gs_bucket


with DAG('Covid-19_ETL',
    default_args=default_args,
    schedule_interval="@daily",
    concurrency=5,
    catchup=True) as dag:

    start_pipeline = DummyOperator(
    task_id = 'start_pipeline'
    )

    download_csv = BashOperator(
        do_xcom_push=False,
        task_id="download_csv",
        bash_command= "wget https://covid.ourworldindata.org/data/owid-covid-data.csv -P /opt/airflow/plugins"
    )

    local_to_gcs = LocalFilesystemToGCSOperator(
        gcp_conn_id='local_to_gcs',
        src='/opt/airflow/plugins/owid-covid-data.csv',
        dst='covid-19.csv'
        bucket=gs_bucket
    )

    start_pipeline >> download_csv >> local_to_gcs











