from datetime import datetime

import boto3
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.eks import (
 EksCreateClusterOperator,
 EksDeleteClusterOperator,
 EksPodOperator,
)
from airflow.utils.task_group import TaskGroup

# Replace the values below with the desired cluster and nodegroup names
CLUSTER_NAME = "my-cluster"
NODEGROUP_NAME = "my-nodegroup"

# Prior to running, in Airflow create a connection of type `aws` with your AWS credentials
# You can follow the steps here: https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/aws.html
# You can set the credentials in airflow_settings.yaml to automatically create the connection for local dev
# Replace the value below with the connection ID you created
AWS_CONN_ID = "aws_conn"
AWS_REGION = "us-east-2"

# If an EKS role for the cluster and managed nodegroup already exists, replace the values with those ARNs
# If not:
# - Follow the steps to create the cluster one here: https://docs.aws.amazon.com/eks/latest/userguide/service_IAM_role.html#create-service-role
# - Follow the steps to create the nodegroup role here: https://docs.aws.amazon.com/eks/latest/userguide/create-node-role.html#create-worker-node-role
# - Replace the ARNs below with the ARNs of the roles you created
# - Since this is a POC, I attached the AmazonEKSCNINodePolicy to the nodegroup role
CLUSTER_ROLE_ARN = "arn:aws:iam::771371893023:role/KubeRay_Data_Team"
NODEGROUP_ROLE_ARN = "arn:aws:iam::771371893023:role/KubeRay_Data_Team"

# Replace the subnet IDs with the subnet IDs in your VPC
# Remember that they need to be in different availability zones
# If you need to create a VPC, follow the steps here: https://docs.aws.amazon.com/eks/latest/userguide/create-public-private-vpc.html
SUBNET_IDS = ["subnet-0c2203a5541163b91", "subnet-0bdedabbfff5dcc85"]

# On Astro, we enforce IMDSv2 for security reasons
# To enable IMDSv2, you need to create a launch template with the MetadataOptions enabled
LAUNCH_TEMPLATE_NAME = "my-launch-template"


with DAG(
    dag_id="setup_teardown_eks",
    schedule=None,
    start_date=datetime(2023, 11, 1),
    catchup=False,
) as dag:

    with TaskGroup("setup_cluster") as setup:
        
        @task
        def create_launch_template():
            """Create a launch template with the MetadataOptions enabled to use IMDSv2"""
            aws_hook = BaseHook.get_connection(AWS_CONN_ID)
            boto3.client(
                "ec2",
                region_name=AWS_REGION,
                aws_access_key_id=aws_hook.login,
                aws_secret_access_key=aws_hook.password,
            ).create_launch_template(
                LaunchTemplateName=LAUNCH_TEMPLATE_NAME,
                LaunchTemplateData={
                    "MetadataOptions": {
                        "HttpEndpoint": "enabled",
                        "HttpTokens": "required",
                    },
                },
            )

        create_launch_template = create_launch_template()

        create_cluster = EksCreateClusterOperator(
            task_id="create_cluster_and_nodegroup",
            cluster_name=CLUSTER_NAME,
            cluster_role_arn=CLUSTER_ROLE_ARN,
            resources_vpc_config={"subnetIds": SUBNET_IDS},
            compute="nodegroup",
            nodegroup_name=NODEGROUP_NAME,
            nodegroup_role_arn=NODEGROUP_ROLE_ARN,
            create_nodegroup_kwargs={"launchTemplate": {"name": LAUNCH_TEMPLATE_NAME}},
            deferrable=False,
            wait_for_completion=True,
            region=AWS_REGION,
            aws_conn_id=AWS_CONN_ID,
        )

        setup_tasks = create_launch_template.as_setup() >> create_cluster.as_setup()

        start_pod = EksPodOperator(
            task_id="start_pod",
            pod_name="test_pod",
            cluster_name="my-cluster",
            image="amazon/aws-cli:latest",
            cmds=["sh", "-c", "echo Test Airflow; date"],
            labels={"demo": "hello_world"},
            region=AWS_REGION,
            aws_conn_id=AWS_CONN_ID,
        )

    with TaskGroup("teardown_cluster") as teardown:

        @task
        def delete_launch_template():
            aws_hook = BaseHook.get_connection(AWS_CONN_ID)
            boto3.client(
                "ec2",
                region_name=AWS_REGION,
                aws_access_key_id=aws_hook.login,
                aws_secret_access_key=aws_hook.password,
            ).delete_launch_template(LaunchTemplateName=LAUNCH_TEMPLATE_NAME)

        delete_launch_template = delete_launch_template()

        delete_nodegroup_and_cluster = EksDeleteClusterOperator(
            task_id="delete_nodegroup_and_cluster",
            cluster_name=CLUSTER_NAME,
            force_delete_compute=True,
            wait_for_completion=True,
            deferrable=False,
            region=AWS_REGION,
            aws_conn_id=AWS_CONN_ID,
        )

        teardown_tasks = (
            delete_nodegroup_and_cluster.as_teardown()
            >> delete_launch_template.as_teardown()
        )

    # setup/teardown dependencies
    create_launch_template >> delete_launch_template
    create_cluster >> delete_nodegroup_and_cluster

    # task dependencies
    setup_tasks >> start_pod >> teardown_tasks

