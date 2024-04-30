
from airflow import DAG
from providers.ray.operators.kuberay import RayClusterOperator,SubmitRayJob
from providers.ray.operators.eks import CreateEKSCluster,DeleteEKSCluster
from datetime import datetime, timedelta
import os

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
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'run_gpu_task',
    default_args=default_args,
    description='Setup EKS cluster with eksctl and run a gpu task on the ray cluster',
    schedule_interval=None,
)

CLUSTERNAME = 'RayCluster'
REGION = 'us-east-2'
K8SPEC = '/usr/local/airflow/dags/scripts/k8-gpu.yaml'
RAY_SPEC = '/usr/local/airflow/dags/scripts/ray-gpu.yaml'
RAY_SVC = '/usr/local/airflow/dags/scripts/ray-service.yaml'
RAY_SCRIPTS = '/usr/local/airflow/dags/ray_scripts'
kubeconfig_directory = f"/tmp/airflow_kubeconfigs/{REGION}/{CLUSTERNAME}/"
os.makedirs(kubeconfig_directory, exist_ok=True)  # Ensure the directory exists
KUBECONFIG_PATH = os.path.join(kubeconfig_directory, "kubeconfig.yaml")


create_eks_cluster = CreateEKSCluster(task_id="CreateEKSCluster",
                                      cluster_name=CLUSTERNAME,
                                      region=REGION,
                                      eks_k8_spec=K8SPEC,
                                      kubeconfig_path= KUBECONFIG_PATH,
                                      env= {},
                                      dag = dag,)

ray_cluster = RayClusterOperator(task_id="RayClusterOperator",
                                 cluster_name=CLUSTERNAME,
                                 region=REGION,
                                 ray_namespace="ray",
                                 ray_cluster_yaml=RAY_SPEC,
                                 ray_svc_yaml= RAY_SVC,
                                 kubeconfig= KUBECONFIG_PATH,
                                 ray_gpu=True,
                                 env = {},
                                 dag = dag,)

submit_ray_job = SubmitRayJob(task_id="SubmitRayJob",
                              url = "{{ task_instance.xcom_pull(task_ids='RayClusterOperator', key='dashboard') }}",
                              entrypoint='python script-gpu.py',
                              wd=RAY_SCRIPTS,
                              env = {},
                              dag = dag,)

delete_eks_cluster = DeleteEKSCluster(task_id="DeleteEKSCluster",
                                      cluster_name=CLUSTERNAME,
                                      region=REGION,
                                      env = {},
                                      dag = dag,)

#create_eks_cluster >> ray_cluster >> submit_ray_job
create_eks_cluster.as_setup() >> ray_cluster >> submit_ray_job >> delete_eks_cluster.as_teardown()
create_eks_cluster >> delete_eks_cluster
