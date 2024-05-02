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
                 entrypoint: str,
                 runtime_env: dict,
                 host: str = None,
                 num_cpus: int = 0,
                 num_gpus: int = 0,
                 memory: int = 0,
                 node_group: str = None,
                 **kwargs,) -> None:

        self.memory = memory
        self.node_group = node_group

        if host is None:
            host = os.getenv['RAY_DASHBOARD_URL']

        super().__init__(
            host = host,
            entrypoint = entrypoint,
            runtime_env = runtime_env,
            num_cpus = num_cpus,
            num_gpus = num_gpus,
            memory = memory,
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
