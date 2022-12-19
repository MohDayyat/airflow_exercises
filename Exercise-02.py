from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def python_first_function():
    
    current_time = datetime.now()
    print("Time now is : ",current_time)
        
        

default_dag_args = { 
    'start_date': datetime(2022, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': 1 }        
        


with DAG("first_python_dag", schedule_interval = '@daily', catchup=False, default_args = default_dag_args) as dag_python:

    task_0 = PythonOperator(task_id = "first_python_task", python_callable = python_first_function)