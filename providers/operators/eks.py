from __future__ import annotations

from airflow.models import BaseOperator, BaseOperatorLink, XCom
from airflow.exceptions import AirflowException
from airflow.hooks.subprocess import SubprocessHook
from airflow.utils.operator_helpers import context_to_airflow_vars
from airflow.utils.context import Context
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.decorators import task

import os
import shutil
import warnings
from functools import cached_property
from typing import TYPE_CHECKING, Container, Sequence, cast
import logging
import tempfile

import logging
logging.basicConfig(level=logging.DEBUG)

class CreateEKSCluster(BaseOperator):


    def __init__(self,*,
                 cluster_name: str,
                 region: str,
                 eks_k8_spec: str,
                 kubeconfig_path: str,
                 env: dict = None,
                 **kwargs,):
        
        super().__init__(**kwargs)
        self.cluster_name = cluster_name
        self.region = region
        self.eks_k8_spec = eks_k8_spec
        self.kubeconfig_path = kubeconfig_path
        self.env = env
        self.output_encoding: str = "utf-8"
        self.cwd = tempfile.mkdtemp(prefix="tmp")

        if not self.cluster_name:
            raise AirflowException("EKS cluster name is required.")
        if not self.region:
            raise AirflowException("EKS region is required.")
        
        # Check if k8 cluster spec is provided
        if not eks_k8_spec:
            raise AirflowException("K8 Cluster spec is required")
        elif not os.path.isfile(eks_k8_spec):
            raise AirflowException(f"The specified K8 cluster YAML file does not exist: {eks_k8_spec}")
        elif not eks_k8_spec.endswith('.yaml') and not eks_k8_spec.endswith('.yml'):
            raise AirflowException("The specified K8 cluster YAML file must have a .yaml or .yml extension.")
        else:
            self.eks_k8_spec = eks_k8_spec

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
            if eksctl get cluster --name {self.cluster_name} > /dev/null 2>&1; then
                echo "Cluster already exists."
            else
                eksctl create cluster -f {self.eks_k8_spec}
            fi
            """
        result = self.execute_bash_command(command,env)
        logging.info(result)

        return result

    def update_kubeconfig(self, env: dict):

        command = f"eksctl utils write-kubeconfig --cluster={self.cluster_name} --region={self.region} --kubeconfig {self.kubeconfig_path}"
        
        result = self.execute_bash_command(command, env)
        logging.info(result)
        return result

    def execute(self, context: Context):

        env = self.get_env(context)
        
        self.create_eks_cluster(env)

        self.update_kubeconfig(env)

        return
    

class DeleteEKSCluster(BaseOperator):


    def __init__(self,*,
                 cluster_name: str = None,
                 region: str = None,
                 env: dict = None,
                 **kwargs,):
        
        super().__init__(**kwargs)
        self.cluster_name = cluster_name
        self.region = region
        self.env = env
        self.output_encoding: str = "utf-8"
        self.cwd = tempfile.mkdtemp(prefix="tmp")

        if not self.cluster_name:
            raise AirflowException("EKS cluster name is required.")
        if not self.region:
            raise AirflowException("EKS region is required.")

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
    
    def delete_eks_cluster(self, env: dict):

        command = f"eksctl delete cluster --name={self.cluster_name} --region={self.region}"
        
        result = self.execute_bash_command(command, env)
        logging.info(result)
        return result

    def execute(self, context: Context):

        env = self.get_env(context)
        
        self.delete_eks_cluster(env)

        return