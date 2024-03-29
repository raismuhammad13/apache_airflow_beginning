from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator


# Assigining default parameters
default_args = {
    'owner':'rais',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}

# Initializing dag
with DAG(
    dag_id="our_first_dag_v6",
    default_args=default_args,
    description="This is our first dag that we write.",
    start_date=datetime(2024,3,25,2),
    schedule_interval="@daily"
) as dag:
    # Creating our first task
    task1 = BashOperator(
        task_id='first_task',
        bash_command='echo hello world, this is the first task!'
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command='echo hello world, this is the second task and run after task1'
    )

    task3 = BashOperator(
        task_id='third_task',
        bash_command='echo hey, I am task3 and will be running after task1 at the same time as task2!'
    )

    # Task dependency method First method for task dependency
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)

    # Task dependency method Second method
    # task1>>task2
    # task1>>task3

    # Task dependency method third method
    task1 >> [task2, task3]