
from airflow import DAG
from ..operators.kuberay import RayClusterOperator
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
    'run_ray_job',
    default_args=default_args,
    description='Setup EKS cluster with eksctl and deploy KubeRay operator',
    schedule_interval='@daily',
)

ray_cluster = RayClusterOperator(eks_cluster_name="RayCluster",
                                 eks_region="us-east-2",
                                 eks_node_type="m5.2xlarge",
                                 eks_min_nodes=1,
                                 eks_max_nodes=3,
                                 eks_nodes=2,
                                 eks_namespace="ray",
                                 ray_cluster_yaml="/usr/local/airflow/dags/ray.yaml",
                                 dag = dag,)

ray_cluster

