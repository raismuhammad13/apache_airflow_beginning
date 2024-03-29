from airflow import DAG, task
from datetime import datetime, timedelta


default_args = {
    "owner":'Rais',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}


with DAG(
    task_id = 'dag_with_taskflow_api_v01',
    default_args = default_args,
    desctiption = 'This dag is created using taskflow api of apache airflow',
    start_date = datetime(2024,3,27,2),
    schedule_interval = '@daily'
) as dag:
    pass