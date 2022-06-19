from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from datetime import datetime, timedelta
from config.config import project_id, staging_dataset, dwh_dataset, gs_bucket

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
    catchup=False) as dag:

    start_pipeline = DummyOperator(
    task_id = 'start_pipeline'
    )

    download_csv = BashOperator(
        do_xcom_push=False,
        task_id="download_csv",
        bash_command="wget https://covid.ourworldindata.org/data/owid-covid-data.csv -O /opt/airflow/plugins/owid-covid-data.csv"
    )

    local_to_gcs = LocalFilesystemToGCSOperator(
        task_id='local_to_gcs',
        gcp_conn_id="gcs_conn_id",
        src='/opt/airflow/plugins/owid-covid-data.csv',
        dst='covid-19.csv',
        bucket=gs_bucket
    )

    check_gcs_file = GCSObjectExistenceSensor(
        task_id='check_gcs_file',
        bucket=gs_bucket,
        google_cloud_conn_id="gcs_conn_id",
        object='covid-19.csv'
    )

    load_to_bigquery = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        gcp_conn_id="gcs_conn_id",
        bucket=gs_bucket,
        source_objects=['covid-19.csv'],
        destination_project_dataset_table=f"{staging_dataset}.covid_data",
        source_format='csv',
        autodetect=True,
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE'
    )

    build_data_mart = BigQueryInsertJobOperator(
        task_id="build_data_mart",
        gcp_conn_id="gcs_conn_id",
        configuration={
            "query":{
                "query": "{% include './sql/covid.sql' %}",
                "useLegacySql": False,
            }
        },
        params={
            'project_id': project_id,
            'staging_dataset': staging_dataset,
            'dwh_dataset': dwh_dataset
            },

    )

    start_pipeline >> download_csv >> local_to_gcs >> check_gcs_file >> load_to_bigquery >> build_data_mart












