
import os
from airflow import DAG
from datetime import datetime, timedelta
from ray_provider.operators.ray import SetupRayCluster, DeleteRayCluster,SubmitRayJob

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=0),
}

CLUSTERNAME = 'RayCluster'
REGION = 'us-east-2'
K8SPEC = '/usr/local/airflow/dags/scripts/k8.yaml'
RAY_SPEC = '/usr/local/airflow/dags/scripts/ray.yaml'
RAY_SVC = '/usr/local/airflow/dags/scripts/ray-service.yaml'
RAY_RUNTIME_ENV = {"working_dir": '/usr/local/airflow/dags/ray_scripts'}

dag = DAG(
    'Setup_Teardown',
    default_args=default_args,
    description='Setup Ray cluster and submit a job',
    schedule_interval=None,
)

setup_cluster = SetupRayCluster(task_id="SetupRayCluster",
                                conn_id = "ray_conn",
                                 ray_cluster_yaml=RAY_SPEC,
                                 ray_svc_yaml= RAY_SVC,
                                 use_gpu=False,
                                 dag = dag,)

submit_ray_job = SubmitRayJob(task_id="SubmitRayJob",
                              conn_id="ray_conn",
                              entrypoint='python script.py',
                              runtime_env= RAY_RUNTIME_ENV,
                              num_cpus=1,
                              num_gpus=0,
                              memory=0,
                              resources={},
                              xcom_task_key = "SetupRayCluster.dashboard",
                              dag = dag,)


"""delete_cluster = DeleteRayCluster(task_id="DeleteRayCluster",
                                  conn_id = "ray_conn",
                                 ray_cluster_yaml=RAY_SPEC,
                                 ray_svc_yaml= RAY_SVC,
                                 use_gpu=False,
                                 dag = dag,)"""
setup_cluster >> submit_ray_job
# Create ray cluster and submit ray job 
#setup_cluster.as_setup() >> submit_ray_job >> delete_cluster.as_teardown()
#setup_cluster >> delete_cluster

