from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime


default_args = {
    'start_date': datetime(2020, 3, 1)
}

with DAG('parallel_dag',
    default_args=default_args,
    schedule_interval="@daily",
    dag_concurrency=8,
    max_active_runs_per_dag=4
    catchup=False) as dag:

    



