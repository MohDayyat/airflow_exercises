import requests 
import time 
import json 
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.python import BranchPythonOperator 
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta 
import pandas as pd 
import numpy as np 
import os

def get_data(**kwargs):
    
    ticker = kwargs['ticker']
    api_key = "7XGTD1I2K282U62M"
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol='+ ticker +'&apikey='+ api_key
    r = requests.get(url)
    
    try :
        data = r.json()
        path = "/Users/Mohammad_Dayyat/exe1/lake_data"
        with open(path + "stock_market_data_" +ticker +"_" + str(time.time()), "w") as ofile:
            json.dump(data,ofile)
    
    except:
        pass
    
        
default_dag_args = { 
    'start_date': datetime(2022, 9, 1), 
    'email_on_failure': False, 
    'email_on_retry': False, 
    'retries': 1, 
    'retry_delay': timedelta(minutes=5), 
    'project_id': 1 }

with DAG("market_data_alphavantage_dag", schedule_interval = '@daily', catchup=False, default_args = default_dag_args) as dag_python:
    task_0 = PythonOperator(task_id = "get_market_data", python_callable = get_data, op_kwargs = {'ticker' : 'IBM'})