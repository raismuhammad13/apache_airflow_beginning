from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator


default_args = {
    "owner":"Rais",
    "retries":5,
    "retry_interval":timedelta(minutes=3)
}


# def greet():
#     return print("Hello World!")

# Python function with parameters
# def greet(name, age):
#     return print(f"Hello world! My name is {name} and I am {age} years old.")

# Taking value from other task in this case task2
def greet(ti):
    first_name=ti.xcom_pull(task_ids="get_name", key='first_name')
    last_name=ti.xcom_pull(task_ids="get_name", key='last_name')
    age = ti.xcom_pull(task_ids="get_age", key='age')
    return print(f"Hello world! My name is {first_name} {last_name} and I am {age} years old.")

# def get_name():
#     return 'Rais'


# Pushing data to xcom
def get_name(ti):
    ti.xcom_push(key='first_name', value='Rais')
    ti.xcom_push(key='last_name', value='Muhammad')

def get_age(ti):
    ti.xcom_push(key="age", value=832)

with DAG(
    dag_id = "dag_with_python_operator_v5",
    default_args=default_args,
    description="Created dag using python operator",
    start_date=datetime(2024,3,27,2),
    schedule_interval="@daily"
) as dag:
    task1 = PythonOperator(
        task_id="greet",
        python_callable=greet,
        # use when python function accepts parameters
        # op_kwargs={"name":"Rais", "age":35}

        # # Value of name is taken from other task so we pass only one parameter
        # op_kwargs={"age":35}

    )

    task2 = PythonOperator(
        task_id = "get_name",
        python_callable=get_name
    )

    task3 = PythonOperator(
        task_id = "get_age",
        python_callable = get_age
    )

    [task2,task3]>>task1