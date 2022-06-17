from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from datetime import datetime

default_args = {
    'start_date': datetime(2020, 3, 1),
    'retries' : 2,
    'retry_delay' : dt.timedelta(minutes=1),
    'email_on_retry' : False,
    'email_on_failure' : False
}

with DAG('Covid-19_ETL',
    default_args=default_args,
    schedule_interval="@daily",
    dag_concurrency=8,
    max_active_runs_per_dag=4,
    catchup=False) as dag:

    download_csv = BashOperator(
        do_xcom_push=False,
        task_id="download_csv",
        bash_command=
    )













