
from airflow import DAG
from operators.kuberay import RayClusterOperator_
from operators.eks import CreateEKSCluster,DeleteEKSCluster
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

CLUSTERNAME = 'RayCluster'
REGION = 'us-east-2'

dag = DAG(
    'start_ray_cluster',
    default_args=default_args,
    description='Setup EKS cluster with eksctl and deploy KubeRay operator',
    schedule_interval='@daily',
)

create_eks_cluster = CreateEKSCluster(task_id="CreateEKSCluster",
                                      cluster_name=CLUSTERNAME,
                                      region=REGION,
                                      eks_k8_spec="/usr/local/airflow/scripts/k8.yaml",
                                      env= {},
                                      dag = dag,)

ray_cluster = RayClusterOperator_(task_id="RayClusterOperator",
                                 cluster_name=CLUSTERNAME,
                                 region=REGION,
                                 ray_namespace="ray",
                                 ray_cluster_yaml="/usr/local/airflow/scripts/ray.yaml",
                                 ray_dashboard_svc_yaml="../scripts/ray-dashboard-service.yaml",
                                 ray_client_svc_yaml="../scripts/ray-client-service.yaml",
                                 env = {},
                                 dag = dag,)

delete_eks_cluster = DeleteEKSCluster(task_id="DeleteEKSCluster",
                                      cluster_name=CLUSTERNAME,
                                      region=REGION,
                                      env = {},
                                      dag = dag,)

create_eks_cluster.as_setup() >> ray_cluster >> delete_eks_cluster.as_teardown()
create_eks_cluster >> delete_eks_cluster

