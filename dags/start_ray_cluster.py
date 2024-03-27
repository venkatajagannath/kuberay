
from airflow import DAG
from operators.kuberay import RayClusterOperator
from datetime import datetime, timedelta

from airflow.models.connection import Connection

# Define the AWS connection
conn = Connection(
    conn_id="aws_conn",
    conn_type="aws",
    extra={
        "config_kwargs": {
            "signature_version": "unsigned",
        },
    },
)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'start_ray_cluster',
    default_args=default_args,
    description='Setup EKS cluster with eksctl and deploy KubeRay operator',
    schedule_interval='@daily',
)

ray_cluster = RayClusterOperator(task_id="RayClusterOperator",
                                 cluster_name="RayCluster",
                                 region="us-east-2",
                                 eks_k8_spec="/usr/local/airflow/scripts/k8.yaml",
                                 ray_namespace="ray",
                                 ray_cluster_yaml="/usr/local/airflow/scripts/ray.yaml",
                                 eks_delete_cluster=False,
                                 env = {},
                                 dag = dag,)

ray_cluster

