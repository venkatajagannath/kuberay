
from airflow import DAG
from providers.ray.operators.kuberay import RayClusterOperator_,SubmitRayJob
from providers.ray.operators.eks import CreateEKSCluster,DeleteEKSCluster
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
RAY_SVC = '/usr/local/airflow/dags/scripts/ray-service.yaml'
RAY_SCRIPTS = '/usr/local/airflow/dags/ray_scripts'

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
                                 ray_svc_yaml= RAY_SVC,
                                 env = {},
                                 dag = dag,)
submit_ray_job = SubmitRayJob(task_id="SubmitRayJob",
                              url = "{{ task_instance.xcom_pull(task_ids='RayClusterOperator', key='url1') }}",
                              entrypoint='python script.py',
                              wd=RAY_SCRIPTS,
                              env = {},
                              dag = dag,)

delete_eks_cluster = DeleteEKSCluster(task_id="DeleteEKSCluster",
                                      cluster_name=CLUSTERNAME,
                                      region=REGION,
                                      env = {},
                                      dag = dag,)

create_eks_cluster.as_setup() >> ray_cluster >> submit_ray_job >> delete_eks_cluster.as_teardown()
create_eks_cluster >> delete_eks_cluster

