import datetime

from airflow import models
from airflow.providers.google.cloud.operators.bigquery import BigQueryListDatasetsOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Define your GCP Project ID
GCP_PROJECT_ID = "gcp-5ann00-sbx-devops"  # Replace with your actual GCP Project ID

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": days_ago(1),
    "project_id": GCP_PROJECT_ID,
}

def _print_datasets(**kwargs):
    """
    A Python function to print the list of BigQuery datasets retrieved by BigQueryListDatasetsOperator.
    The result of the BigQueryListDatasetsOperator is pushed to XCom.
    """
    ti = kwargs["ti"]
    dataset_ids = ti.xcom_pull(task_ids="list_bigquery_datasets")
    if dataset_ids:
        print(f"Found the following BigQuery datasets in project {GCP_PROJECT_ID}:")
        for dataset in dataset_ids:
            print(f"- {dataset}")
    else:
        print(f"No BigQuery datasets found in project {GCP_PROJECT_ID}.")

with models.DAG(
    dag_id="gcp_list_bigquery_datasets_example",
    default_args=default_args,
    schedule_interval=None,  # Run manually
    catchup=False,
    tags=["gcp", "bigquery"],
) as dag:
    # Task 1: List all BigQuery datasets in the specified project
    list_bigquery_datasets = BigQueryListDatasetsOperator(
        task_id="list_bigquery_datasets",
        project_id=GCP_PROJECT_ID,
        # The BigQueryListDatasetsOperator pushes the list of dataset IDs to XCom
        # under the key 'return_value' by default.
    )

    # Task 2: Print the list of datasets retrieved from the previous task
    print_dataset_list = PythonOperator(
        task_id="print_dataset_list",
        python_callable=_print_datasets,
    )

    # Define task dependencies
    list_bigquery_datasets >> print_dataset_list
