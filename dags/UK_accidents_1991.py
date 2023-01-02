from datetime import datetime
from airflow import DAG 
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from scripts.clean import clean
from scripts.hospitals import hospitals
from scripts.encode import encode
from scripts.save_to_accidents_db import save
from scripts.dashboard import dashboard


with DAG(
    dag_id= "accidents_1991",
    start_date=datetime(2022, 12, 24),
    schedule="30 18 * * *",
) as dag :

    hello = BashOperator(task_id="hello", bash_command="echo starting")

    path1 = "/opt/airflow/dags/files/1991_Accidents_UK.csv"
    cleaning  = PythonOperator(task_id="clean",python_callable=clean,op_kwargs={"path" :path1})

    path2 = '/opt/airflow/dags/files/clean.csv'
    get_hospitals  = PythonOperator(task_id="hospitals_around",python_callable=hospitals,op_kwargs={"path" :path2})

    path3 = '/opt/airflow/dags/files/hospitals.csv'
    encoding  = PythonOperator(task_id="encode",python_callable=encode,op_kwargs={"path" :path3})

    path4 = '/opt/airflow/dags/files/encoded.parquet'
    saving  = PythonOperator(task_id="save_to_db",python_callable=save,op_kwargs={"path" :path4})

    path5 = '/opt/airflow/dags/files/hospitals.csv'
    ploting  = PythonOperator(task_id="dashboard",python_callable=dashboard,op_kwargs={"path" :path5})

    # Set dependencies between tasks
    # hello -> cleaning -> hospitals -> ploting
    #                               \-> encoding -> saving

    hello >> cleaning >> get_hospitals 
    get_hospitals.set_downstream(ploting)
    get_hospitals.set_downstream(encoding)
    encoding.set_downstream(saving)
    
