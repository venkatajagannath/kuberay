


"""from airflow.decorators import dag
from operators.kuberay import RayClusterExecutor, SubmitRayJob
import datetime

#Define the basic parameters of the DAG, like schedule and start_date
with dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example"],
) as dag:
    
    start_cluster = RayClusterOperator()


    submit_job = SubmitRayJob()



    delete_cluster = RayClusterOperator()



    start_cluster>>submit_job>>delete_cluster"""

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sh

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
    'start_date': datetime(2024, 3, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'run_ray_job',
    default_args=default_args,
    description='Setup EKS cluster with eksctl and deploy KubeRay operator',
    schedule_interval='@daily',
)

eksctl_create_cluster = BashOperator(
    task_id='create_eks_cluster',
    bash_command="""
        eksctl create cluster \
        --name RayCluster \
        --region us-east-2 \
        --node-type m5.2xlarge \
        --nodes 2 \
        --nodes-min 1 \
        --nodes-max 3 \
       --managed
    """,
    dag=dag,
)

# Task to generate kubeconfig
generate_kubeconfig = BashOperator(
        task_id='generate_kubeconfig',
        bash_command=f"""
        eksctl utils write-kubeconfig --cluster=RayCluster --region=us-east-2
        """,
        dag = dag,
    )

# Task to install using Helm
helm_install = BashOperator(
    task_id='helm_install',
    bash_command=f"""
    helm repo add kuberay https://ray-project.github.io/kuberay-helm/&&
    helm repo update &&
    helm install kuberay-operator kuberay/kuberay-operator --version 1.0.0 --create-namespace
    """,
    dag = dag,
)

def print_working_directory():
    print("Current Working Directory:", os.getcwd())

get_cwd_task = PythonOperator(
        task_id='print_cwd',
        python_callable=print_working_directory
    )

apply_ray_cluster_spec = BashOperator(
    task_id='apply_ray_cluster_spec',
    bash_command='kubectl apply -f ./dags/ray.yaml',
    dag=dag,
)

eksctl_delete_cluster = BashOperator(
    task_id='eksctl_delete_cluster',
    bash_command="""
        eksctl delete cluster --name RayCluster --region us-east-2
    """,
    dag=dag,
)

eksctl_create_cluster >> generate_kubeconfig >> helm_install >> get_cwd_task >> apply_ray_cluster_spec >> eksctl_delete_cluster
