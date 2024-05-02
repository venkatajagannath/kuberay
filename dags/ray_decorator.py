from airflow.decorators import dag, task
from providers.ray.operators.kuberay import RayClusterOperator, SubmitRayJob
from providers.ray.operators.eks import CreateEKSCluster, DeleteEKSCluster
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
    def create_eks_cluster():
        # The function body can replicate what the operator would do, with appropriate modifications
        operator = CreateEKSCluster(
            task_id = "Create_cluster",
            cluster_name = CLUSTERNAME,
            region = REGION,
            eks_k8_spec = K8SPEC,
            kubeconfig_path = KUBECONFIG_PATH
        )
        return operator.execute()

    @task
    def ray_cluster(create_cluster):
        operator = RayClusterOperator(
            task_id="Ray_cluster_operator",
            cluster_name = CLUSTERNAME,
            region = REGION,
            ray_namespace='ray',
            ray_cluster_yaml = RAY_SPEC,
            ray_svc_yaml = RAY_SVC,
            kubeconfig = KUBECONFIG_PATH,
            ray_gpu=True
        )
        return operator.execute()

    @task
    def submit_ray_job(host):
        operator = SubmitRayJob(
            task_id = "Submit_ray_job",
            host=host["dashboard"],
            entrypoint='python script-gpu.py',
            runtime_env={"working_dir": '/usr/local/airflow/dags/ray_scripts'},
            num_cpus=1,
            num_gpus=1,
            memory=0
        )
        return operator.execute()

    @task
    def delete_eks_cluster(submit_job):
        operator = DeleteEKSCluster(
            task_id = "delete_eks_cluster",
            cluster_name='RayCluster',
            region='us-east-2'
        )
        return operator.execute()

    create_cluster = create_eks_cluster()
    ray = ray_cluster(create_cluster)
    submit_job = submit_ray_job(ray)
    delete_eks_cluster(submit_job)

gpu_dag = taskflow_gpu_task()
