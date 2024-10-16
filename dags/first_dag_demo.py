from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task

with DAG(
    dag_id="demo",
    start_date= datetime(2024,10,16),
    schedule=None
):
    hello = BashOperator(
        task_id="hello",
        bash_command="echo hello"
    )

    @task()
    def airflow():
        print("airflow")

    hello >> airflow
