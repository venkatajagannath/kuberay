


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
    
    start_cluster = RayClusterExecutor()


    submit_job = SubmitRayJob()



    delete_cluster = RayClusterExecutor()



    start_cluster>>submit_job>>delete_cluster"""

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.amazon.aws.operators.eks import EksCreateClusterOperator,EksDeleteClusterOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.docker.operators.docker import DockerOperator
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

"""eksctl_create_cluster = DockerOperator(
  task_id='eksctl_create_cluster',
  image='public.ecr.aws/eksctl/eksctl',
  container_name='eksctl',
  command = "eksctl create cluster \
        --name RayCluster \
        --region us-east-2 \
        --node-type m5.2xlarge \
        --nodes 2 \
        --nodes-min 1 \
        --nodes-max 3 \
        --managed",
  api_version='auto',
  auto_remove=True,
  docker_url='unix://var/run/docker.sock',
  network_mode="bridge",
  tty=True,
  xcom_all=False,
  mount_tmp_dir=False,
  environment=dict(os.environ),
  dag = dag,
)"""

#create_cluster = BashOperator(
#    task_id='create_eks_cluster',
#    bash_command="""
#        docker run --rm -it public.ecr.aws/eksctl/eksctl \
#        eksctl create cluster \
#        --name my-eks-cluster \
#        --region us-east-2 \
#        --node-type m5.2xlarge \
#        --nodes 2 \
#        --nodes-min 1 \
#        --nodes-max 3 \
#       --managed
#    """,
#    dag=dag,
#)

# Create an instance of EksCreateClusterOperator
eksctl_create_cluster = EksCreateClusterOperator(
        task_id='create_eks_cluster',
        cluster_name="RayCluster",
        cluster_role_arn="arn:aws:iam::771371893023:role/KubeRay_Data_Team",
        nodegroup_name = "KubeRay",
        nodegroup_role_arn='arn:aws:iam::771371893023:role/KubeRay_Data_Team',
        resources_vpc_config={
        'subnetIds': ['subnet-0e184b23aec13ff41', 'subnet-0c13f543fea1e5eb4','subnet-0099de4f8ad7a42e1','subnet-026d89c844dec4b2a'],
        'securityGroupIds': ['sg-0b16a58d6c5446e47']},
        wait_for_completion=True,
        region="us-east-2",
        aws_conn_id = conn.conn_id,
        dag = dag,
    )

update_kubeconfig = BashOperator(
    task_id='update_kubeconfig',
    bash_command='aws eks update-kubeconfig --region us-east-2 --name RayCluster --alias kuberay-profile',
    dag=dag,
)

check_install_helm = BashOperator(
    task_id='check_install_helm',
    bash_command="""
        if ! command -v helm &> /dev/null
        then
            curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
            chmod 700 get_helm.sh
            ./get_helm.sh
        else
            echo "Helm is already available."
        fi
    """,
    dag=dag,
)

add_kuberay_operator = BashOperator(
    task_id='add_kuberay_operator',
    bash_command="""
        helm repo add kuberay https://ray-project.github.io/kuberay-helm/
        helm repo update
        helm install kuberay-operator kuberay/kuberay-operator --version 1.0.0
    """,
    dag=dag,
)

apply_ray_cluster_spec = BashOperator(
    task_id='apply_ray_cluster_spec',
    bash_command='kubectl --context=kuberay-profile apply -f ./ray.yaml',
    dag=dag,
)

delete_cluster = EksDeleteClusterOperator(
    task_id="delete_cluster",
    cluster_name="RayCluster",
)

eksctl_create_cluster >> update_kubeconfig >> check_install_helm >> add_kuberay_operator >> apply_ray_cluster_spec >> delete_cluster
