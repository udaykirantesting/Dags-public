from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

# Simple GCP + GKE sample DAG
#
# What it does:
# 1) Lists objects in a GCS bucket (GCSListObjectsOperator)
# 2) Runs a short Python/ bash job inside a Kubernetes pod on the same GKE cluster
#    (KubernetesPodOperator). The pod prints a message and can access GCS if Workload
#    Identity or proper credentials are configured.
#
# Notes / prerequisites:
# - This DAG assumes Airflow is running in-cluster on the same GKE cluster (in_cluster=True).
# - Recommended: Use Workload Identity to allow the Kubernetes ServiceAccount to impersonate
#   a Google service account. Add the annotation below:
#     annotations={"iam.gke.io/gcp-service-account": "my-gsa@PROJECT_ID.iam.gserviceaccount.com"}
# - If not using Workload Identity, ensure the pod has access to credentials via secret
#   or node/service credentials (not recommended for production).
# - Make sure the Airflow environment has the google provider package installed:
#     pip install apache-airflow-providers-google
#   and the kubernetes provider:
#     pip install apache-airflow-providers-cncf-kubernetes
#
# Replace my-gcs-bucket and the annotation/service-account with your values.

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="gcp_gke_simple_sample",
    default_args=default_args,
    description="Simple sample DAG demonstrating GCS -> run job on GKE (KubernetesPodOperator)",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["gcp", "gke", "example"],
) as dag:

    # 1) List objects in a GCS bucket. This operator will push the list to XCom.
    list_gcs_objects = GCSListObjectsOperator(
        task_id="list_gcs_objects",
        bucket="my-gcs-bucket",  # <- change this
        prefix="input/",         # optional
        delimiter=None,
        # google_cloud_storage_conn_id="google_cloud_default",  # change if needed
        xcom_push=True,
    )

    # 2) Run a pod on the GKE cluster. The pod is started in-cluster (Airflow running in same cluster).
    run_on_gke = KubernetesPodOperator(
        namespace="default",
        image="python:3.10-slim",
        cmds=["bash", "-cx"],
        # demonstrate reading XCom and printing; real workloads would mount creds or use Workload Identity
        arguments=[
            "echo 'Hello from KubernetesPodOperator running on GKE'; "
            "python - <<'PY'\n"
            "import os, json\n"
            "gcs_objects = os.environ.get('GCS_OBJECTS')\n"
            "print('GCS_OBJECTS env var (from XCom):', gcs_objects)\n"
            "PY"
        ],
        labels={"app": "airflow-gke-sample"},
        name="print-gcs-and-hello",
        task_id="run_on_gke",
        get_logs=True,
        in_cluster=True,             # important for Airflow running inside the same cluster
        is_delete_operator_pod=True,
        # If using Workload Identity, annotate the pod to use a GSA:
        annotations={
            # replace with your GSA for Workload Identity
            "iam.gke.io/gcp-service-account": "my-gsa@PROJECT_ID.iam.gserviceaccount.com"
        },
        # pass the GCS list from previous task into the pod via env var templated at runtime
        env_vars={
            # KubernetesPodOperator can template values from XCom using Airflow's templating:
            # here we set GCS_OBJECTS to the JSON-serialised XCom returned by list_gcs_objects.
            "GCS_OBJECTS": "{{ ti.xcom_pull(task_ids='list_gcs_objects') | tojson }}",
        },
        # optionally set resource requests/limits, volumes, volume_mounts etc.
    )

    list_gcs_objects >> run_on_gke
