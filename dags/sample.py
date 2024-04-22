from datetime import datetime
from airflow.models import DAG
from airflow.decorators import dag, task

# Set up the DAG with default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

# Define the DAG using decorators
@dag(dag_id='simple_taskflow_dag', schedule_interval='@daily', default_args=default_args, catchup=False)
def simple_dag():
    # Task to generate data
    @task
    def generate_data():
        return "Hello, World!"

    # Task to run a ray job
    @task.ray(worker_queue ="gpu_workers",namespace="ray")
    def process_data(data):

        import ray

        @ray.remote
        def hello_world():
            return data

        ray.init()
        print(ray.get(hello_world.remote()))
        return

    # Set task dependencies
    data = generate_data()
    process_data(data)

# Instantiate the DAG
dag_instance = simple_dag()

