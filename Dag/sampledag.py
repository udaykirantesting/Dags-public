from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

# Very simple DAG: a single task that prints a message.
# Drop-in replacement for the original complex example when you just need a minimal DAG.

with DAG(
    dag_id="gcp_gke_simple_sample",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["example"],
) as dag:

    hello = BashOperator(
        task_id="hello",
        bash_command="echo 'Hello from Airflow - very simple DAG!'",
    )

    hello
