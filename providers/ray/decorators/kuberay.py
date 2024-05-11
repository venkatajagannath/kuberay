from __future__ import annotations

import os
import uuid
import textwrap
import shutil
from tempfile import mkdtemp
from typing import TYPE_CHECKING, Callable, Sequence
from tempfile import TemporaryDirectory
from airflow.utils.types import NOTSET
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
        super().__init__(**kwargs)
        self.config = config
        self.node_group = node_group
        self.host = self.config.get('host', os.getenv('RAY_DASHBOARD_URL'))
        self.entrypoint = self.config.get('entrypoint')
        self.runtime_env = self.config.get('runtime_env', {})
        self.num_cpus = self.config.get('num_cpus')
        self.num_gpus = self.config.get('num_gpus')
        self.memory = self.config.get('memory')

    def execute(self, context: Context):
        with TemporaryDirectory(prefix="ray") as tmp_dir:
            py_source = self.get_python_source().splitlines()
            function_body = textwrap.dedent('\n'.join(py_source))

            self.logger.info(function_body)

            script_filename = os.path.join(tmp_dir, "script.py")
            with open(script_filename, "w") as file:
                file.write(f"{function_body}\n{self.extract_function_name()}()")
            
            self.entrypoint = f'python {script_filename}'
            self.runtime_env['working_dir'] = tmp_dir

            self.logger.info("Running ray job...")
            try:
                result = super().execute(context)
            except Exception as e:
                self.log.error(f"Failed during execution with error: {e}")
                raise AirflowException("Job submission failed")

        return result

    def extract_function_name(self):
        # Assuming the function name can be extracted from the source
        return self.get_python_source().split('def ')[1].split('(')[0].strip()
        
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
