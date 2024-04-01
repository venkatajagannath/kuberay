
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
K8SPEC = '/usr/local/airflow/dags/scripts/k8.yaml'
RAY_SPEC = '/usr/local/airflow/dags/scripts/ray.yaml'
RAY_DASHBOARD = '/usr/local/airflow/dags/scripts/ray-dashboard-service.yaml'
RAY_CLIENT = '/usr/local/airflow/dags/scripts/ray-client-service.yaml'

dag = DAG(
    'start_ray_cluster',
    default_args=default_args,
    description='Setup EKS cluster with eksctl and deploy KubeRay operator',
    schedule_interval='@daily',
)

create_eks_cluster = CreateEKSCluster(task_id="CreateEKSCluster",
                                      cluster_name=CLUSTERNAME,
                                      region=REGION,
                                      eks_k8_spec=K8SPEC,
                                      env= {},
                                      dag = dag,)

ray_cluster = RayClusterOperator_(task_id="RayClusterOperator",
                                 cluster_name=CLUSTERNAME,
                                 region=REGION,
                                 ray_namespace="ray",
                                 ray_cluster_yaml=RAY_SPEC,
                                 ray_dashboard_svc_yaml=RAY_DASHBOARD,
                                 ray_client_svc_yaml=RAY_CLIENT,
                                 env = {},
                                 dag = dag,)

delete_eks_cluster = DeleteEKSCluster(task_id="DeleteEKSCluster",
                                      cluster_name=CLUSTERNAME,
                                      region=REGION,
                                      env = {},
                                      dag = dag,)

create_eks_cluster.as_setup() >> ray_cluster >> delete_eks_cluster.as_teardown()
create_eks_cluster >> delete_eks_cluster

