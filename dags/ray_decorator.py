from airflow.decorators import dag, task
from providers.ray.operators.kuberay import RayClusterOperator, SubmitRayJob
from providers.ray.operators.eks import CreateEKSCluster, DeleteEKSCluster
from providers.ray.decorators.kuberay import ray_task
from datetime import datetime, timedelta
import os

CLUSTERNAME = 'RayCluster'
REGION = 'us-east-2'
K8SPEC = '/usr/local/airflow/dags/scripts/k8-gpu.yaml'
RAY_SPEC = '/usr/local/airflow/dags/scripts/ray-gpu.yaml'
RAY_SVC = '/usr/local/airflow/dags/scripts/ray-service.yaml'
RAY_RUNTIME_ENV = {"working_dir": '/usr/local/airflow/dags/ray_scripts'}
kubeconfig_directory = f"/tmp/airflow_kubeconfigs/{REGION}/{CLUSTERNAME}/"
os.makedirs(kubeconfig_directory, exist_ok=True)  # Ensure the directory exists
KUBECONFIG_PATH = os.path.join(kubeconfig_directory, "kubeconfig.yaml")

@dag(
    'ray_decorator',
    start_date=datetime(2024, 3, 26),
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    schedule_interval=None,
    description='Setup EKS cluster with eksctl and run a gpu task on the ray cluster'
)

def taskflow_gpu_task():
    
    @task
    def submit_ray_job():
        operator = SubmitRayJob(
            task_id = "Submit_ray_job",
            host="http://a24b5747b7474435aa7abe4681ec6f49-1521101610.us-east-2.elb.amazonaws.com:8265/",
            entrypoint='python script-gpu.py',
            runtime_env={"working_dir": '/usr/local/airflow/dags/ray_scripts'},
            num_cpus=1,
            num_gpus=0,
            memory=0,
            resources={},
        )
        return operator.execute({})
    
    @ray_task(host="http://a24b5747b7474435aa7abe4681ec6f49-1521101610.us-east-2.elb.amazonaws.com:8265/",
              entrypoint='python script-gpu.py',runtime_env={"working_dir": '/usr/local/airflow/dags/ray_scripts'},num_cpus=1,num_gpus=0)
    def ray_decorator_task():
        return

    
    submit_ray_job()

gpu_dag = taskflow_gpu_task()
