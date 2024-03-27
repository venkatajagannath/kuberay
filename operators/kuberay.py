
from __future__ import annotations

from airflow.models import BaseOperator, BaseOperatorLink, XCom
from airflow.exceptions import AirflowException
from airflow.hooks.subprocess import SubprocessHook
from airflow.utils.operator_helpers import context_to_airflow_vars
from airflow.utils.context import Context

import os
import shutil
import warnings
from functools import cached_property
from typing import TYPE_CHECKING, Container, Sequence, cast
import logging
import tempfile

import logging
logging.basicConfig(level=logging.DEBUG)
logging.debug("Importing kuberay.py")


class RayClusterOperator(BaseOperator):


    def __init__(self,*,
                 eks_cluster_name: str = None,
                 eks_region: str = None,
                 eks_node_type: str = None,
                 eks_nodes: int = None,
                 eks_min_nodes: int = None,
                 eks_max_nodes: int = None,
                 eks_namespace: str = None,
                 ray_cluster_yaml : str = None,
                 eks_delete_cluster: bool = False,
                 env: dict = None,
                 **kwargs,):
        
        super().__init__(**kwargs)
        self.eks_cluster_name = eks_cluster_name
        self.eks_region = eks_region
        self.eks_node_type = eks_node_type
        self.eks_nodes = str(eks_nodes)
        self.eks_min_nodes = str(eks_min_nodes)
        self.eks_max_nodes = str(eks_max_nodes)
        self.eks_namespace = eks_namespace
        self.eks_delete_cluster = eks_delete_cluster
        self.env = env

        if not self.eks_cluster_name:
            raise AirflowException("EKS cluster name is required.")
        if not self.eks_region:
            raise AirflowException("EKS region is required.")
        if not self.eks_node_type:
            raise AirflowException("EKS node type is required.")
        if not self.eks_nodes:
            raise AirflowException("EKS number of nodes is required.")
        if not self.eks_min_nodes:
            raise AirflowException("EKS minimum number of nodes is required.")
        if not self.eks_max_nodes:
            raise AirflowException("EKS maximum number of nodes is required.")
        if not self.eks_namespace:
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

        self.output_encoding: str = "utf-8"
        self.cwd = tempfile.mkdtemp(prefix="tmp")

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
        eksctl create cluster \
        --name {self.eks_cluster_name} \
        --region {self.eks_region} \
        --node-type {self.eks_node_type} \
        --nodes {self.eks_nodes} \
        --nodes-min {self.eks_min_nodes} \
        --nodes-max {self.eks_max_nodes} \
        --managed
        """
        result = self.execute_bash_command(command,env)
        logging.info(result.output)

        return result

    def update_kubeconfig(self, env: dict):

        command = f"eksctl utils write-kubeconfig --cluster={self.eks_cluster_name} --region={self.eks_region}"
        
        result = self.execute_bash_command(command, env)
        logging.info(result.output)
        return result

    def create_ray_cluster(self, env: dict):

        command = f"kubectl apply -f {self.ray_cluster_yaml}"
        
        result = self.execute_bash_command(command, env)
        logging.info(result.output)
        return result

    def add_kuberay_operator(self, env: dict):
        # Helm commands to add repo, update, and install KubeRay operator
        helm_commands = f"""
        helm repo add kuberay https://ray-project.github.io/kuberay-helm/ && \
        helm repo update && \
        helm install kuberay-operator kuberay/kuberay-operator \
        --version 1.0.0 --create-namespace --namespace {self.namespace}
        """
        
        result = self.execute_bash_command(helm_commands, env)
        logging.info(result.output)
        return result
    
    def delete_eks_cluster(self, env: dict):

        command = f"eksctl delete cluster --name={self.eks_cluster_name} --region={self.eks_region}"
        
        result = self.execute_bash_command(command, env)
        logging.info(result.output)
        return result

    def execute(self, context: Context):

        env = self.get_env(context)

        if self.eks_delete_cluster:
            self.delete_eks_cluster(env)
            return
        
        self.create_eks_cluster(env)
        
        self.update_kubeconfig(env)

        self.add_kuberay_operator(env)

        self.create_ray_cluster(env)

        self.delete_eks_cluster(env)

        return


class SubmitRayJob(BaseOperator):
    def __init__(self):
        pass

    def execute(self):
        pass


class DeployRayService(BaseOperator):
    def __init__(self):
        pass

    def execute(self):
        pass