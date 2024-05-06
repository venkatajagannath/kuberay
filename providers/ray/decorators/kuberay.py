from __future__ import annotations

import base64
import os
import uuid
from shlex import quote
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Callable, Sequence, Any, Collection, Mapping

from airflow.decorators.base import DecoratedOperator, TaskDecorator, task_decorator_factory
from airflow.utils.context import Context, context_merge
from airflow.utils.operator_helpers import determine_kwargs
from airflow.utils.types import NOTSET

from providers.ray.operators.kuberay import SubmitRayJob

if TYPE_CHECKING:
    from airflow.utils.context import Context


class _RayDecoratedOperator(DecoratedOperator, SubmitRayJob):
    custom_operator_name = "@task.ray"

    template_fields: Sequence[str] = (*DecoratedOperator.template_fields, *SubmitRayJob.template_fields)
    template_fields_renderers: dict[str, str] = {
        **DecoratedOperator.template_fields_renderers,
        **SubmitRayJob.template_fields_renderers,
    }

    def __init__(self,
                 config: dict,
                 node_group: str = None,
                 **kwargs,) -> None:

        self.config = config
        self.node_group = node_group

        if host is None:
            host = os.getenv('RAY_DASHBOARD_URL')
        elif 'host' in self.config:
            self.host = self.config['host']
        
        if 'entrypoint' in self.config:
            self.entrypoint = self.config['entrypoint']

        if 'runtime_env' in self.config:
            self.runtime_env = self.config['runtime_env']
        
        if 'num_cpus' in self.config:
            self.num_cpus = self.config['num_cpus']
        
        if 'num_gpus' in self.config:
            self.num_gpus = self.config['num_gpus']
        
        if 'memory' in self.config:
            self.memory = self.config['memory']

        super().__init__(
            host = self.host,
            entrypoint = self.entrypoint,
            runtime_env = self.runtime_env,
            num_cpus = self.num_cpus,
            num_gpus = self.num_gpus,
            memory = self.memory,
            **kwargs,
        )

    def execute(self, context: Context):

        if self.node_group:
            self.resources = {self.node_group:0.1}
        
        return super().execute(context)
    

def ray_task(
        python_callable: Callable | None = None,
        multiple_outputs: bool | None = None,
        **kwargs,
) -> TaskDecorator:
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_RayDecoratedOperator,
        **kwargs
    )
