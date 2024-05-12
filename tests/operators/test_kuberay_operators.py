import unittest
from unittest.mock import patch, MagicMock
from airflow.utils.context import Context
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.models.taskinstance import TaskInstance
from providers.operators.kuberay import SubmitRayJob, RayClusterOperator

class TestRayClusterOperator(unittest.TestCase):

    def setUp(self):
        self.operator = RayClusterOperator(
            cluster_name="test-cluster",
            region="us-east-2",
            kubeconfig='./dags/scripts/k8-gpu.yaml',
            ray_namespace="ray",
            ray_cluster_yaml='./dags/scripts/ray-gpu.yaml',
            ray_svc_yaml='./dags/scripts/ray-service.yaml',
            task_id='test_ray_cluster_operator',
            ray_gpu=True
        )

    @patch('providers.operators.kuberay.RayClusterOperator.subprocess_hook')
    def test_add_kuberay_operator_successful(self, mock_subprocess_hook):
        """Test the addition of the KubeRay operator via Helm commands."""
        mock_subprocess_hook.return_value.run_command.return_value = MagicMock(exit_code=0, output='successful')
        result = self.operator.add_kuberay_operator({})
        self.assertIn('successful', result)

    @patch('providers.operators.kuberay.RayClusterOperator.subprocess_hook')
    def test_create_ray_cluster_successful(self, mock_subprocess_hook):
        """Test successful creation of a Ray cluster via kubectl."""
        mock_subprocess_hook.return_value.run_command.return_value = MagicMock(exit_code=0, output='cluster created')
        result = self.operator.create_ray_cluster({})
        self.assertIn('cluster created', result)

    @patch('providers.operators.kuberay.RayClusterOperator.subprocess_hook')
    def test_execute_bash_command_fail(self, mock_subprocess_hook):
        """Test handling of a failed bash command execution."""
        mock_subprocess_hook.return_value.run_command.return_value = MagicMock(exit_code=1)
        with self.assertRaises(AirflowException):
            self.operator.execute_bash_command('fail command', {})

    def create_mock_context(self):
        """Helper method to create a mocked context including a mock for 'task_instance'."""
        mock_context = Context()
        mock_task_instance = MagicMock(spec=TaskInstance)
        mock_task_instance.xcom_push = MagicMock()
        mock_context['task_instance'] = mock_task_instance
        return mock_context

    @patch('providers.operators.kuberay.RayClusterOperator.create_k8_service')
    @patch('providers.operators.kuberay.RayClusterOperator.add_nvidia_device')
    @patch('providers.operators.kuberay.RayClusterOperator.create_ray_cluster')
    @patch('providers.operators.kuberay.RayClusterOperator.add_kuberay_operator')
    @patch('providers.operators.kuberay.RayClusterOperator.get_env')
    def test_execute(self, mock_get_env, mock_add_operator, mock_create_cluster, mock_add_device, mock_create_service):
        """Test the complete execution flow of the operator."""
        mock_context = self.create_mock_context()
        mock_get_env.return_value = {}
        mock_add_operator.return_value = 'operator added'
        mock_create_cluster.return_value = 'cluster created'
        mock_add_device.return_value = 'device added'
        mock_create_service.return_value = {'port1': 'http://example.com:8080'}
        
        result = self.operator.execute(mock_context)
        
        mock_get_env.assert_called_once()
        mock_add_operator.assert_called_once()
        mock_create_cluster.assert_called_once()
        mock_add_device.assert_called_once()
        mock_create_service.assert_called_once()
        self.assertEqual(result, {'port1': 'http://example.com:8080'})
        mock_context['task_instance'].xcom_push.assert_called()

    @patch('os.path.isfile')
    @patch('builtins.open', new_callable=unittest.mock.mock_open, read_data='data')
    def test_ray_cluster_yaml_file_validation(self, mock_file, mock_isfile):
        """Test validation of the Ray cluster YAML file."""
        mock_isfile.return_value = True
        operator = RayClusterOperator(
            cluster_name="test-cluster",
            region="us-west-1",
            kubeconfig="/path/to/kubeconfig",
            ray_namespace="ray",
            ray_cluster_yaml="/path/to/valid.yaml",
            ray_svc_yaml="/path/to/service.yaml",
            task_id='test_ray_cluster_operator'
        )
        self.assertEqual(operator.ray_cluster_yaml, "/path/to/valid.yaml")


class TestSubmitRayJob(unittest.TestCase):
    def setUp(self):
        self.operator = SubmitRayJob(
            host='http://example.com',
            entrypoint='python task.py',
            runtime_env={'PYTHON_VERSION': '3.11'},
            num_cpus=4,
            num_gpus=1,
            memory=1024,
            task_id='submit_ray_job_test'
        )

        self.operator.client = MagicMock()

    @patch('providers.operators.kuberay.SubmitRayJob.get_current_status')
    @patch('ray.dashboard.modules.job.sdk.JobSubmissionClient', new_callable=MagicMock)
    def test_execute_successful(self, mock_client, mock_get_status):
        mock_job_client = MagicMock()
        mock_job_client.submit_job.return_value = '123'
        mock_client.return_value = mock_job_client

        mock_get_status.return_value = 'SUCCEEDED'
        
        job_id = self.operator.execute(Context())
        
        mock_client.assert_called_with('http://example.com')
        mock_job_client.submit_job.assert_called_once()
        mock_get_status.assert_called_with()
        self.assertEqual(job_id, '123')

    @patch('providers.operators.kuberay.SubmitRayJob.get_current_status')
    @patch('ray.dashboard.modules.job.sdk.JobSubmissionClient')
    def test_execute_fail_on_status(self, mock_client, mock_get_current_status):
        mock_job_client = MagicMock()
        mock_job_client.submit_job.return_value = '123'
        mock_client.return_value = mock_job_client
        mock_get_current_status.return_value = 'FAILED'
        
        with self.assertRaises(AirflowException) as context:
            self.operator.execute(Context())
        
        self.assertTrue("Job failed" in str(context.exception))

    def test_on_kill(self):
        self.operator.client = MagicMock()
        self.operator.job_id = '123'
        self.operator.on_kill()
        self.operator.client.delete_job.assert_called_once_with('123')

    @patch('providers.operators.kuberay.SubmitRayJob.client')
    def test_execute_complete_success(self, mock_client):
        event = {'status': 'success', 'job_id': '123', 'message': 'Job completed successfully'}
        self.operator.execute_complete(Context(), event)
        self.assertEqual(self.operator.job_id, '123')

    @patch('providers.operators.kuberay.SubmitRayJob.client')
    def test_defer_job_polling_called(self, mock_client):
        self.operator.client.get_job_status.return_value = 'PENDING'
        self.operator.job_id = '123'
        with self.assertRaises(TaskDeferred):
            self.operator.execute(Context())
        # Here, you should check for the expected behavior related to the defer method

if __name__ == '__main__':
    unittest.main()
