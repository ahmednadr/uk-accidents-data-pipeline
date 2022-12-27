from datetime import datetime
from airflow import DAG 
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from task_clean_encode import Clean_Encode

with DAG(
    dag_id= "accidents_1991",
    start_date=datetime(2022, 12, 24),
    schedule="30 18 * * *",
) as dag :

    hello = BashOperator(task_id="hello", bash_command="echo starting")

    path = "/opt/airflow/dags/files/1991_Accidents_UK.csv"
    task1  = PythonOperator(task_id="Clean_and_encode",python_callable=Clean_Encode,op_kwargs={"path" :path})

    # Set dependencies between tasks
    hello >> task1