
from __future__ import annotations

from airflow.models import BaseOperator, BaseOperatorLink, XCom
from airflow.exceptions import AirflowException
from airflow.hooks.subprocess import SubprocessHook
from airflow.utils.operator_helpers import context_to_airflow_vars
from airflow.utils.context import Context
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.decorators import task

from providers.ray.triggers.kuberay import RayJobTrigger

import os
import shutil
import warnings
from functools import cached_property
from typing import TYPE_CHECKING, Container, Sequence, cast
import logging
import tempfile
from ray.job_submission import JobSubmissionClient, JobStatus
import time

import logging
logging.basicConfig(level=logging.DEBUG)

from kubernetes import client, config, watch
import yaml
import time

def create_service_and_get_url(namespace="default", yaml_file="ray-head-service.yaml"):
    config.load_kube_config()

    with open(yaml_file) as f:
        service_data = yaml.safe_load(f)

    v1 = client.CoreV1Api()
    created_service = v1.create_namespaced_service(namespace=namespace, body=service_data)
    logging.info(f"Service {created_service.metadata.name} created. Waiting for an external DNS name...")

    max_retries = 30
    retry_interval = 40

    external_dns = None

    for attempt in range(max_retries):
        logging.info(f"Attempt {attempt + 1}: Checking for service's external DNS name...")
        service = v1.read_namespaced_service(name=created_service.metadata.name, namespace=namespace)
        
        if service.status.load_balancer.ingress and service.status.load_balancer.ingress[0].hostname:
            external_dns = service.status.load_balancer.ingress[0].hostname
            logging.info(f"External DNS name found: {external_dns}")
            urls = [f"http://{external_dns}:{port.port}" for port in service.spec.ports]

            for url in urls:
                logging.info(f"Service URL: {url}")
            break
        else:
            logging.info("External DNS name not yet available, waiting...")
            time.sleep(retry_interval)
    
    if not external_dns:
        logging.error("Failed to find the external DNS name for the created service within the expected time.")
        return None
    
    return urls


class RayClusterOperator(BaseOperator):


    def __init__(self,*,
                 cluster_name: str = None,
                 region: str = None,
                 eks_k8_spec: str = None,
                 ray_namespace: str = None,
                 ray_cluster_yaml : str = None,
                 eks_delete_cluster: bool = False,
                 env: dict = None,
                 **kwargs,):
        
        super().__init__(**kwargs)
        self.cluster_name = cluster_name
        self.region = region
        self.ray_namespace = ray_namespace
        self.eks_delete_cluster = eks_delete_cluster
        self.env = env
        self.output_encoding: str = "utf-8"
        self.cwd = tempfile.mkdtemp(prefix="tmp")

        if not self.cluster_name:
            raise AirflowException("EKS cluster name is required.")
        if not self.region:
            raise AirflowException("EKS region is required.")
        if not self.ray_namespace:
            raise AirflowException("EKS namespace is required.")
        
        # Check if k8 cluster spec is provided
        """if not eks_k8_spec:
            raise AirflowException("K8 Cluster spec is required")
        elif not os.path.isfile(eks_k8_spec):
            raise AirflowException(f"The specified K8 cluster YAML file does not exist: {eks_k8_spec}")
        elif not eks_k8_spec.endswith('.yaml') and not eks_k8_spec.endswith('.yml'):
            raise AirflowException("The specified K8 cluster YAML file must have a .yaml or .yml extension.")
        else:
            self.eks_k8_spec = eks_k8_spec"""

        # Check if ray cluster spec is provided
        if not ray_cluster_yaml:
            raise AirflowException("Ray Cluster spec is required")
        elif not os.path.isfile(ray_cluster_yaml):
            raise AirflowException(f"The specified Ray cluster YAML file does not exist: {ray_cluster_yaml}")
        elif not ray_cluster_yaml.endswith('.yaml') and not ray_cluster_yaml.endswith('.yml'):
            raise AirflowException("The specified Ray cluster YAML file must have a .yaml or .yml extension.")
        else:
            self.ray_cluster_yaml = ray_cluster_yaml

    def __del__(self):
        if self.eks_delete_cluster:
            self.delete_eks_cluster()
        
        if os.path.isdir(self.cwd):
            os.rmdir(self.cwd)

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

        logging.info("Running bash command: "+ bash_command)

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
    
    def create_eks_cluster(self,env : dict):

        command = f"""
        eksctl create cluster -f {self.eks_k8_spec}
        """
        result = self.execute_bash_command(command,env)
        logging.info(result)

        return result

    def update_kubeconfig(self, env: dict):

        command = f"eksctl utils write-kubeconfig --cluster={self.cluster_name} --region={self.region}"
        
        result = self.execute_bash_command(command, env)
        logging.info(result)
        return result

    def add_kuberay_operator(self, env: dict):
        # Helm commands to add repo, update, and install KubeRay operator
        helm_commands = f"""
        helm repo add kuberay https://ray-project.github.io/kuberay-helm/ && \
        helm repo update && \
        helm install kuberay-operator kuberay/kuberay-operator \
        --version 1.0.0 --create-namespace --namespace {self.ray_namespace}
        """
        
        result = self.execute_bash_command(helm_commands, env)
        logging.info(result)
        return result
    
    def create_ray_cluster(self, env: dict):

        command = f"kubectl apply -f {self.ray_cluster_yaml} -n {self.ray_namespace}"
        
        result = self.execute_bash_command(command, env)
        logging.info(result)
        return result
    
    def delete_eks_cluster(self, env: dict):

        command = f"eksctl delete cluster --name={self.cluster_name} --region={self.region}"
        
        result = self.execute_bash_command(command, env)
        logging.info(result)
        return result

    def execute(self, context: Context):

        env = self.get_env(context)

        if self.eks_delete_cluster:
            self.delete_eks_cluster(env)
            return
        
        self.update_kubeconfig(env)

        self.add_kuberay_operator(env)

        self.create_ray_cluster(env)

        return

class RayClusterOperator_(BaseOperator):


    def __init__(self,*,
                 cluster_name: str = None,
                 region: str = None,
                 ray_namespace: str = None,
                 ray_cluster_yaml : str = None,
                 ray_svc_yaml : str = None,
                 env: dict = None,
                 **kwargs,):
        
        super().__init__(**kwargs)
        self.cluster_name = cluster_name
        self.region = region
        self.ray_namespace = ray_namespace
        self.ray_svc_yaml = ray_svc_yaml
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

        logging.info("Running bash command: "+ bash_command)

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
    
    def update_kubeconfig(self, env: dict):

        command = f"eksctl utils write-kubeconfig --cluster={self.cluster_name} --region={self.region}"
        
        result = self.execute_bash_command(command, env)
        logging.info(result)
        return result

    def add_kuberay_operator(self, env: dict):
        # Helm commands to add repo, update, and install KubeRay operator
        helm_commands = f"""
        helm repo add kuberay https://ray-project.github.io/kuberay-helm/ && \
        helm repo update && \
        helm install kuberay-operator kuberay/kuberay-operator \
        --version 1.0.0 --create-namespace --namespace {self.ray_namespace}
        """
        
        result = self.execute_bash_command(helm_commands, env)
        logging.info(result)
        return result
    
    def create_ray_cluster(self, env: dict):

        command = f"kubectl apply -f {self.ray_cluster_yaml} -n {self.ray_namespace}"
        
        result = self.execute_bash_command(command, env)
        logging.info(result)
        return result
    
    def create_k8_service(self, namespace: str, yaml : str):

        logging.info("Creating service with yaml file: "+ yaml)

        return create_service_and_get_url(namespace, yaml)
    
    def execute(self, context: Context):

        env = self.get_env(context)

        self.update_kubeconfig(env)

        self.add_kuberay_operator(env)

        self.create_ray_cluster(env)

        logging.info("Creating services ...")

        # Creating K8 services
        urls = self.create_k8_service(self.ray_namespace, self.ray_svc_yaml)

        if urls:
            for index, url in enumerate(urls, start=1):
                key = f'url{index}'
                context['task_instance'].xcom_push(key=key, value=url)
        else:
            # Handle the case when urls is None or empty
            logging.info("No URLs to push to XCom.")

        return

class SubmitRayJob(BaseOperator):

    template_fields = ('url', 'entrypoint', 'wd',)

    def __init__(self,*,
                 url: str,
                 entrypoint: str,
                 wd: str,
                 timeout: int = 600,
                 env: dict = None,
                 **kwargs):
        
        super().__init__(**kwargs)
        self.url = url
        self.entrypoint = entrypoint
        self.timeout = timeout
        self.wd = wd
        self.env = env if env is not None else {}
        self.client = None
        self.job_id = None
        self.status_to_wait_for = {JobStatus.SUCCEEDED, JobStatus.STOPPED, JobStatus.FAILED}
    
    def __del__(self):
        self.client.delete_job(self.job_id)
        return self.client.tail_job_logs(self.job_id)

    def wait_until_status(self, timeout_seconds=5):
        start = time.time()
        while time.time() - start <= timeout_seconds:
            status = self.client.get_job_status(self.job_id)
            logs = self.client.get_job_logs(self.job_id)
            print(logs)
            print(f"status: {status}")
            if status in self.status_to_wait_for:
                break
            time.sleep(1)

    def execute(self,context : Context):

        if not self.client:
            self.client = JobSubmissionClient(f"{self.url}")

        job_id = self.client.submit_job(
            entrypoint= self.entrypoint,
            runtime_env={"working_dir": self.wd})  #https://docs.ray.io/en/latest/ray-core/handling-dependencies.html#runtime-environments
        self.job_id = job_id

        self.defer(
            timeout= self.execution_timeout,
            trigger= RayJobTrigger(
                url = self.url,
                job_id = job_id,
                timeout= self.timeout,
                poll_interval=60
            ),
            method_name="execute_complete",)
        
        return job_id
    
    def execute_complete(self, context: Context, event: Any = None) -> None:

        if event["status"] == JobStatus.FAILED or event["status"] == "timeout":
            raise AirflowException(event["message"])
        elif event["status"] == JobStatus.SUCCEEDED:
            return
        
        return None
        



class DeployRayService(BaseOperator):
    def __init__(self,*,
                 url: str,
                 namespace: str,
                 ray_serve_svc: str,
                 entrypoint: str,
                 wd: str,
                 env: dict = None,
                 **kwargs):
        
        super().__init__(**kwargs)
        self.url = url
        self.entrypoint = entrypoint
        self.namespace = namespace
        self.wd = wd
        self.ray_serve_svc = ray_serve_svc
        self.env = env if env is not None else {}
        self.client = None
        self.job_id = None
        self.status_to_wait_for = {JobStatus.SUCCEEDED, JobStatus.STOPPED, JobStatus.FAILED}

    def wait_until_status(self, timeout_seconds=5):
        start = time.time()
        while time.time() - start <= timeout_seconds:
            status = self.client.get_job_status(self.job_id)
            logs = self.client.get_job_logs(self.job_id)
            print(logs)
            print(f"status: {status}")
            if status in self.status_to_wait_for:
                break
            time.sleep(1)

    def deploy_svc(self, namespace: str, svc_yaml: str):

        logging.info("Creating service with yaml file: "+ yaml)

        return create_service_and_get_url(namespace, yaml)

    def execute(self,context : Context):

        if not self.client:
            self.client = JobSubmissionClient(f"{self.url}")

        job_id = self.client.submit_job(
            entrypoint= self.entrypoint,
            runtime_env={"working_dir": self.wd})
        self.job_id = job_id

        self.wait_until_status()

        self.deploy_svc(self.namespace, self.ray_serve_svc)
        
        return self.client.tail_job_logs(job_id)