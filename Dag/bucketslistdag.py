import datetime

from airflow import models
from airflow.providers.google.cloud.operators.gcs import GCSListBucketsOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# --- Configuration ---
# IMPORTANT: Replace with your actual GCP Project ID
GCP_PROJECT_ID = "gcp-5ann00-sbx-devops"

# --- Python function to process and print results ---
def _process_bucket_list(**kwargs):
    """
    Retrieves the list of bucket names from XCom and prints them.
    """
    ti = kwargs['ti']
    bucket_names = ti.xcom_pull(task_ids='list_gcs_buckets_task')

    if bucket_names:
        print(f"--- GCS Buckets in Project {GCP_PROJECT_ID} ---")
        for bucket in bucket_names:
            print(f"- {bucket}")
        print("------------------------------------------")
    else:
        print(f"No GCS buckets found in project {GCP_PROJECT_ID}.")

# --- DAG Definition ---
with models.DAG(
    dag_id="simple_gcp_list_buckets",
    start_date=days_ago(1),
    schedule_interval=None,  # Run manually
    catchup=False,
    tags=["gcp", "gcs", "simple"],
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=5),
        "project_id": GCP_PROJECT_ID,
    },
) as dag:
    # Task 1: List all GCS buckets in the specified project
    list_gcs_buckets_task = GCSListBucketsOperator(
        task_id="list_gcs_buckets_task",
        project_id=GCP_PROJECT_ID,
        # This operator automatically pushes the list of bucket names to XCom.
    )

    # Task 2: Print the retrieved list of buckets
    print_bucket_list_task = PythonOperator(
        task_id="print_bucket_list_task",
        python_callable=_process_bucket_list,
    )

    # --- Define Task Flow ---
    list_gcs_buckets_task >> print_bucket_list_task
