
from __future__ import annotations

import logging
import os
import shutil
import tempfile
import time
import warnings
import yaml
import requests

from pyhelm.repo import Repo
from pyhelm.chart import ChartBuilder
from pyhelm.tiller import Tiller

from airflow.exceptions import AirflowException
from airflow.hooks.subprocess import SubprocessHook
from airflow.models import BaseOperator, BaseOperatorLink, XCom
from airflow.utils.context import Context
from airflow.utils.decorators import apply_defaults
from airflow.utils.operator_helpers import context_to_airflow_vars
from datetime import timedelta
from functools import cached_property
from kubernetes import client, config, utils
from kubernetes.client.rest import ApiException
from kubernetes.client.api_client import ApiClient
from kubernetes.dynamic import DynamicClient
from logging import Logger
from providers.triggers.kuberay import RayJobTrigger
from providers.utils.kuberay import setup_logging
from ray.job_submission import JobSubmissionClient, JobStatus
from typing import TYPE_CHECKING, Container, Sequence, cast

# Set up logging
logger = setup_logging('kuberay')



def install_or_upgrade_helm_chart(ray_namespace: str):
    # Load Kubernetes configuration
    config.load_kube_config()

    # Initialize Tiller
    tiller = Tiller()

    # Add Helm repository
    repo_url = "https://ray-project.github.io/kuberay-helm/"
    repo_name = "kuberay"
    Repo.add_repo(repo_name, repo_url)
    Repo.update()

    # Define the chart details
    chart_name = "kuberay-operator"
    chart_version = "1.0.0"

    # Build the chart
    chart = ChartBuilder({
        "name": chart_name,
        "source": {
            "type": "repo",
            "location": f"{repo_name}/{chart_name}",
            "version": chart_version
        }
    })

    # Install or upgrade the chart
    tiller.install_release(chart.get_helm_chart(), ray_namespace, dry_run=False)
    print(f"Installed or upgraded chart {chart_name} in namespace {ray_namespace}")


def apply_crd(yaml_file: str):
    # Load kube config
    config.load_kube_config()

    # Load the YAML file
    with open(yaml_file, 'r') as f:
        crd = yaml.safe_load(f)

    # Determine the kind and API version of the CRD
    kind = crd.get('kind')
    api_version = crd.get('apiVersion')

    # Split API version to group and version
    group, _, version = api_version.partition('/')

    # Get the plural and namespaced properties
    plural = crd['metadata']['name'].split('.')[0]
    namespaced = crd.get('spec', {}).get('scope', '') == 'Namespaced'

    # Create an API instance
    if group:
        api_instance = client.CustomObjectsApi()
    else:
        api_instance = client.ApiClient()

    # Define the namespace
    namespace = crd['metadata'].get('namespace', 'default')

    try:
        # Check if the CRD already exists
        existing_crd = api_instance.get_namespaced_custom_object(
            group=group,
            version=version,
            namespace=namespace,
            plural=plural,
            name=crd['metadata']['name']
        )
        # If it exists, update it
        api_instance.replace_namespaced_custom_object(
            group=group,
            version=version,
            namespace=namespace,
            plural=plural,
            name=crd['metadata']['name'],
            body=crd
        )
        print(f"Updated existing {kind} '{crd['metadata']['name']}'")
    except ApiException as e:
        if e.status == 404:
            # If it doesn't exist, create it
            api_instance.create_namespaced_custom_object(
                group=group,
                version=version,
                namespace=namespace,
                plural=plural,
                body=crd
            )
            print(f"Created {kind} '{crd['metadata']['name']}'")
        else:
            raise e

class RayClusterOperator(BaseOperator):

    def __init__(self,*,
                 cluster_name: str,
                 region: str,
                 kubeconfig: str,
                 ray_namespace: str,
                 ray_cluster_yaml : str,
                 ray_svc_yaml : str,
                 ray_gpu: bool = False,
                 env: dict = None,
                 **kwargs):
        
        super().__init__(**kwargs)
        self.cluster_name = cluster_name
        self.region = region
        self.kubeconfig = kubeconfig
        self.ray_namespace = ray_namespace
        self.ray_svc_yaml = ray_svc_yaml
        self.use_gpu = ray_gpu
        self.env = env
        self.output_encoding: str = "utf-8"
        self.cwd = tempfile.mkdtemp(prefix="tmp")

        if not self.cluster_name:
            raise AirflowException("EKS cluster name is required.")
        if not self.region:
            raise AirflowException("EKS region is required.")
        if not self.ray_namespace:
            raise AirflowException("EKS namespace is required.")
        
        # Check if ray cluster spec is provided
        if not ray_cluster_yaml:
            raise AirflowException("Ray Cluster spec is required")
        elif not os.path.isfile(ray_cluster_yaml):
            raise AirflowException(f"The specified Ray cluster YAML file does not exist: {ray_cluster_yaml}")
        elif not ray_cluster_yaml.endswith('.yaml') and not ray_cluster_yaml.endswith('.yml'):
            raise AirflowException("The specified Ray cluster YAML file must have a .yaml or .yml extension.")
        else:
            self.ray_cluster_yaml = ray_cluster_yaml

        if self.kubeconfig:
            os.environ['KUBECONFIG'] = self.kubeconfig
        
        self.k8Client = client.ApiClient()

    @cached_property
    def subprocess_hook(self):
        """Returns hook for running the bash command."""
        return SubprocessHook()

    def get_env(self, context):
        """Build the set of environment variables to be exposed for the bash command."""
        system_env = os.environ.copy()
        env = self.env
        if env is None:
            env = system_env
        else:
            system_env.update(env)
            env = system_env

        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        self.log.debug(
            "Exporting env vars: %s",
            " ".join(f"{k}={v!r}" for k, v in airflow_context_vars.items()),
        )
        env.update(airflow_context_vars)
        return env

    def execute_bash_command(self, bash_command:str, env: dict):
        
        bash_path = shutil.which("bash") or "bash"

        self.log.info("Running bash command: "+ bash_command)

        result = self.subprocess_hook.run_command(
            command=[bash_path, "-c", bash_command],
            env=env,
            output_encoding=self.output_encoding,
            cwd=self.cwd,
        )

        if result.exit_code != 0:
            raise AirflowException(
                f"Bash command failed. The command returned a non-zero exit code {result.exit_code}."
            )

        return result.output


    def add_kuberay_operator(self, env: dict):
        # Helm commands to add repo, update, and install KubeRay operator
        helm_commands = f"""
                    helm repo add kuberay https://ray-project.github.io/kuberay-helm/ && \
                    helm repo update && \
                    helm upgrade --install kuberay-operator kuberay/kuberay-operator \
                    --version 1.0.0 --create-namespace --namespace {self.ray_namespace}
                    """
        result = self.execute_bash_command(helm_commands, env)
        self.log.info(result)
        return result
    
    def create_ray_cluster(self, env: dict):

        command = f"kubectl apply -f {self.ray_cluster_yaml} -n {self.ray_namespace}"
        
        result = self.execute_bash_command(command, env)
        self.log.info(result)
        return result
    
    def add_nvidia_device(self,env: dict):

        command = "kubectl apply -f https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v0.9.0/nvidia-device-plugin.yml"

        result = self.execute_bash_command(command,env)
        self.log.info(result)
        return result
    
    def create_k8_service(self, namespace: str ="default", yaml_file: str ="ray-head-service.yaml"):

        self.log.info("Creating service with yaml file: "+ yaml_file)

        config.load_kube_config(self.kubeconfig)

        with open(yaml_file) as f:
            service_data = yaml.safe_load(f)

        v1 = client.CoreV1Api()
        created_service = v1.create_namespaced_service(namespace=namespace, body=service_data)
        logger.info(f"Service {created_service.metadata.name} created. Waiting for an external DNS name...")

        max_retries = 30
        retry_interval = 40

        external_dns = None

        for attempt in range(max_retries):
            logger.info(f"Attempt {attempt + 1}: Checking for service's external DNS name...")
            service = v1.read_namespaced_service(name=created_service.metadata.name, namespace=namespace)
            
            if service.status.load_balancer.ingress and service.status.load_balancer.ingress[0].hostname:
                external_dns = service.status.load_balancer.ingress[0].hostname
                logger.info(f"External DNS name found: {external_dns}")
                break
            else:
                logger.info("External DNS name not yet available, waiting...")
                time.sleep(retry_interval)

        if not external_dns:
            logger.error("Failed to find the external DNS name for the created service within the expected time.")
            return None
        
        # Wait for the endpoints to be ready
        for attempt in range(max_retries):
            endpoints = v1.read_namespaced_endpoints(name=created_service.metadata.name, namespace=namespace)
            if endpoints.subsets and all([subset.addresses for subset in endpoints.subsets]):
                logger.info("All associated pods are ready.")
                break
            else:
                logger.info(f"Pods not ready, waiting... (Attempt {attempt + 1})")
                time.sleep(retry_interval)
        else:
            logger.error("Pods failed to become ready within the expected time.")
            raise AirflowException("Pods failed to become ready within the expected time.")

        # Assuming all ports in the service need to be accessed
        urls = {port.name: f"http://{external_dns}:{port.port}" for port in service.spec.ports}
        for port_name, url in urls.items():
            logger.info(f"Service URL for {port_name}: {url}")

        return urls
    
    def execute(self, context: Context):

        env = self.get_env(context)

        #self.add_kuberay_operator(env)

        install_or_upgrade_helm_chart(self.ray_namespace)

        self.create_ray_cluster(env)

        if self.use_gpu:
            self.add_nvidia_device(env)

        if self.ray_svc_yaml:
            # Creating K8 services
            urls = self.create_k8_service(self.ray_namespace, self.ray_svc_yaml)

            if urls:
                for key,value in urls.items():
                    context['task_instance'].xcom_push(key=key, value=value)
            else:
                # Handle the case when urls is None or empty
                self.log.info("No URLs to push to XCom.")

        return urls

class SubmitRayJob(BaseOperator):

    template_fields = ('host','entrypoint','runtime_env','num_cpus','num_gpus','memory')

    def __init__(self,*,
                 host: str,
                 entrypoint: str,
                 runtime_env: dict,
                 num_cpus: int = 0,
                 num_gpus: int = 0,
                 memory: int | float = 0,
                 resources: dict = None, 
                 timeout: int = 600,
                 **kwargs):
        
        super().__init__(**kwargs)
        self.host = host
        self.entrypoint = entrypoint
        self.runtime_env = runtime_env
        self.num_cpus = num_cpus
        self.num_gpus = num_gpus
        self.memory = memory
        self.resources = resources
        self.timeout = timeout
        self.client = None
        self.job_id = None
        self.status_to_wait_for = {JobStatus.SUCCEEDED, JobStatus.STOPPED, JobStatus.FAILED}

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
    
    def __del__(self):
        if self.client:   
            return self.client.delete_job(self.job_id)
        else:
            return
        
    def on_kill(self):
        if self.client:   
            return self.client.delete_job(self.job_id)
        else:
            return
    def execute(self,context : Context):

        if not self.client:
            self.logger.info(f"URL is: {self.host}")
            self.client = JobSubmissionClient(f"{self.host}")

        self.job_id = self.client.submit_job(
            entrypoint= self.entrypoint,
            runtime_env=self.runtime_env, #https://docs.ray.io/en/latest/ray-core/handling-dependencies.html#runtime-environments
            entrypoint_num_cpus = self.num_cpus,
            entrypoint_num_gpus = self.num_gpus,
            entrypoint_memory = self.memory,
            entrypoint_resources = self.resources)  
        
        self.logger.info(f"Ray job submitted with id:{self.job_id}")

        current_status = self.get_current_status()
        if current_status in (JobStatus.RUNNING, JobStatus.PENDING):
            self.logger.info("Deferring the polling to RayJobTrigger...")
            self.defer(
                timeout= timedelta(hours=1),
                trigger= RayJobTrigger(
                    host = self.host,
                    job_id = self.job_id,
                    end_time= time.time() + self.timeout,
                    poll_interval=2
                ),
                method_name="execute_complete",)
        elif current_status == JobStatus.SUCCEEDED:
            self.logger.info("Job %s completed successfully", self.job_id)
            return
        elif current_status == JobStatus.FAILED:
            raise AirflowException(f"Job failed:\n{self.job_id}")
        elif current_status == JobStatus.STOPPED:
            raise AirflowException(f"Job was cancelled:\n{self.job_id}")
        else:
            raise Exception(f"Encountered unexpected state `{current_status}` for job_id `{self.job_id}")
        
        return self.job_id
    
    def get_current_status(self):
        
        job_status = self.client.get_job_status(self.job_id)
        self.logger.info(f"Current job status for {self.job_id} is: {job_status}")
        return job_status
    
    def execute_complete(self, context: Context, event: Any = None) -> None:

        if event["status"] == "error" or event["status"] == "cancelled":
            self.logger.info(f"Ray job {self.job_id} execution not completed...")
            raise AirflowException(event["message"])
        elif event["status"] == "success":
            self.logger.info(f"Ray job {self.job_id} execution succeeded ...")
            return None