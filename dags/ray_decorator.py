from airflow.decorators import dag, task
from datetime import datetime, timedelta
import os
from ray_provider.decorators.ray import ray_task

RAY_TASK_CONFIG = {
    'conn_id': 'ray_job',
    'runtime_env': {
        "working_dir": '/usr/local/airflow/dags/ray_scripts',
        "pip": ["numpy"]
    },
    'num_cpus': 1,
    'num_gpus': 0,
}

@dag(
    'ray_decorator',
    start_date=datetime(2024, 3, 26),
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    schedule_interval=None,
    description='Run a ray task on the cluster'
)
def taskflow_task():
    
    @ray_task(config=RAY_TASK_CONFIG)
    def ray_decorator_task(number):

        import ray

        @ray.remote
        def hello_world():
            return f"{number} -- hello world"

        ray.init()
        print(ray.get(hello_world.remote()))
    
    ray_decorator_task(123)
cpu_dag = taskflow_task()