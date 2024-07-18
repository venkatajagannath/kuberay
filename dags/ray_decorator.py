from airflow.decorators import dag, task
from datetime import datetime, timedelta
import os
from ray_provider.decorators.ray import ray_task
import ray
import numpy as np

RAY_RUNTIME_ENV = {"working_dir": '/usr/local/airflow/dags/ray_scripts',
                   "pip": ["numpy"]}
RAY_TASK_CONFIG = {
    'conn_id': 'ray_job',
    'entrypoint': 'python script.py',
    'runtime_env': RAY_RUNTIME_ENV,
    'num_cpus': 1,
    'num_gpus': 0,
    'memory': 0
}

@dag(
    'ray_multi_task_dag',
    start_date=datetime(2024, 3, 26),
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    schedule_interval=None,
    description='Multi-task Ray DAG for data processing and analysis'
)
def taskflow_ray_multi_task():

    @ray_task(config=RAY_TASK_CONFIG)
    def generate_data(num_points):
        @ray.remote
        def create_point():
            return np.random.rand(2)

        ray.init()
        points = ray.get([create_point.remote() for _ in range(num_points)])
        return points

    @ray_task(config=RAY_TASK_CONFIG)
    def calculate_distances(points):
        @ray.remote
        def compute_distance(point):
            return np.linalg.norm(point)

        ray.init()
        distances = ray.get([compute_distance.remote(point) for point in points])
        return distances

    @ray_task(config=RAY_TASK_CONFIG)
    def analyze_results(distances):
        @ray.remote
        def compute_stats(data):
            return {
                "mean": np.mean(data),
                "median": np.median(data),
                "std_dev": np.std(data)
            }

        ray.init()
        stats = ray.get(compute_stats.remote(distances))
        print("Distance analysis results:")
        print(f"Mean: {stats['mean']:.4f}")
        print(f"Median: {stats['median']:.4f}")
        print(f"Standard Deviation: {stats['std_dev']:.4f}")
        return stats

    # Define the task dependencies
    points = generate_data(1000)
    distances = calculate_distances(points)
    analyze_results(distances)

ray_multi_task_dag = taskflow_ray_multi_task()