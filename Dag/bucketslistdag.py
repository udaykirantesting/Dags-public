import datetime

from airflow import models
from airflow.providers.google.cloud.operators.gcs import GCSListBucketsOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

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

def _print_buckets(**kwargs):
    """
    A Python function to print the list of buckets retrieved by GCSListBucketsOperator.
    The result of the GCSListBucketsOperator is pushed to XCom.
    """
    ti = kwargs["ti"]
    bucket_names = ti.xcom_pull(task_ids="list_gcs_buckets")
    if bucket_names:
        print(f"Found the following GCS buckets in project {GCP_PROJECT_ID}:")
        for bucket in bucket_names:
            print(f"- {bucket}")
    else:
        print(f"No GCS buckets found in project {GCP_PROJECT_ID}.")

with models.DAG(
    dag_id="gcp_list_buckets_example",
    default_args=default_args,
    schedule_interval=None,  # Run manually
    catchup=False,
    tags=["gcp", "gcs"],
) as dag:
    # Task 1: List all GCS buckets in the specified project
    list_gcs_buckets = GCSListBucketsOperator(
        task_id="list_gcs_buckets",
        project_id=GCP_PROJECT_ID,
        # The GCSListBucketsOperator pushes the list of bucket names to XCom
        # under the key 'return_value' by default.
    )

    # Task 2: Print the list of buckets retrieved from the previous task
    print_bucket_list = PythonOperator(
        task_id="print_bucket_list",
        python_callable=_print_buckets,
    )

    # Define task dependencies
    list_gcs_buckets >> print_bucket_list
