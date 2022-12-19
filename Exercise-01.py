from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import os


default_dag_args = { 
    'start_date': datetime(2022, 1, 1), 
    'email_on_failure': False, 
    'email_on_retry': False, 
    'retries': 1, 
    'retry_delay': timedelta(minutes=5), 
    'project_id': 1 }

with DAG("First_DAG", schedule_interval = None, default_args = default_dag_args) as dag:

    source_file = "/Users/Mohammad_Dayyat/exe1/lake_data/dataset_raw.txt"
    destenation_file = "/Users/Mohammad_Dayyat/exe1/clean_data"
    
    task_0 = BashOperator(task_id = 'bash_task', bash_command = "echo 'command executed from Bash Operator' ")
    task_1 = BashOperator(task_id = 'bash_task_move_data', bash_command = "copy {source_file} {destenation_file}")
    task_2 = BashOperator(task_id = 'bash_task_remove_data', bash_command = "os.remove({source_file}) ")


task_0 >> task_1 >> task_2
