from airflow.decorators import dag, task
from datetime import datetime, timedelta
import os
from ray_provider.decorators.ray import task

RAY_TASK_CONFIG = {
    'conn_id': 'ray_job',
    'entrypoint': 'python script.py',
    'runtime_env': {"working_dir": '/usr/local/airflow/dags/ray_scripts',
                   "pip": ["numpy"]},
    'num_cpus': 1,
    'num_gpus': 0,
    'memory': 0,
    'poll_interval': 5
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
    
    @task.ray(config=RAY_TASK_CONFIG)
    def ray_decorator_task(number):

        import ray

        @ray.remote
        def hello_world(number):
            return f"{number} -- hello world"

        ray.init()
        print(ray.get(hello_world.remote(number)))
    
    ray_decorator_task(123)

cpu_dag = taskflow_task()