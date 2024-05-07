from __future__ import annotations

import os
import uuid
import textwrap
import shutil
from tempfile import mkdtemp
from typing import TYPE_CHECKING, Callable, Sequence
from tempfile import TemporaryDirectory
from airflow.decorators.base import DecoratedOperator, task_decorator_factory
from airflow.utils.context import Context
from airflow.exceptions import AirflowException
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

    def __init__(self, config: dict, node_group: str = None, **kwargs) -> None:
        self.config = config
        self.node_group = node_group
        self.host = self.config.get('host', os.getenv('RAY_DASHBOARD_URL'))
        self.entrypoint = self.config.get('entrypoint', None)
        self.runtime_env = self.config.get('runtime_env', {})
        self.num_cpus = self.config.get('num_cpus', None)
        self.num_gpus = self.config.get('num_gpus', None)
        self.memory = self.config.get('memory', None)

        super().__init__(
            host=self.host,
            entrypoint=self.entrypoint,
            runtime_env=self.runtime_env,
            num_cpus=self.num_cpus,
            num_gpus=self.num_gpus,
            memory=self.memory,
            **kwargs,
        )

    def execute(self, context: Context):
        if self.node_group:
            self.resources = {self.node_group: 0.1}

        py_source = self.get_python_source().splitlines()
        function_body = textwrap.dedent('\n'.join(py_source[1:]))

        self.logger.info(function_body)

        return super().execute(context)

        """
        tmp_dir = None

        try:
            # Create a temporary directory manually
            tmp_dir = mkdtemp(prefix="ray")

            script_filename = os.path.join(tmp_dir, "script.py")
            with open(script_filename, "w") as file:
                file.write(function_body)

            #self.entrypoint = f'python script.py'
            #self.runtime_env['working_dir'] = tmp_dir

            self.logger.info("Running ray job...")
            result = super().execute(context)

        except Exception as e:
            self.log.error(f"Failed during execution with error: {e}")
            raise AirflowException("Job submission failed")

        finally:
            # Clean up the temporary directory if it was created
            if tmp_dir and os.path.exists(tmp_dir):
                shutil.rmtree(tmp_dir)

        return result
"""
        
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
