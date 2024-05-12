import unittest
from unittest.mock import patch, MagicMock
import asyncio
from datetime import datetime
from providers.triggers.kuberay import RayJobTrigger
from ray.dashboard.modules.job.sdk import JobStatus

class TestRayJobTrigger(unittest.TestCase):
    def setUp(self):
        self.trigger = RayJobTrigger(job_id="123", host="http://localhost:8080", end_time=1700000000.0)

    @patch('ray.dashboard.modules.job.sdk.JobSubmissionClient')  # Mock the JobSubmissionClient
    async def test_no_job_id(self, mock_client):
        trigger = RayJobTrigger(job_id="", host="http://localhost:8080", end_time=1700000000.0)
        event_gen = trigger.run()
        async for event in self.trigger.run():
            self.assertEqual(event, {"status": "error", "message": "No job_id provided to async trigger", "job_id": ""})

    @patch('asyncio.sleep', return_value=None)
    @patch('ray.dashboard.modules.job.sdk.JobSubmissionClient')
    async def test_job_success_before_timeout(self, mock_client_class, mock_sleep):
        mock_client = MagicMock()
        mock_client.get_job_status.return_value = JobStatus.SUCCEEDED
        mock_client_class.return_value = mock_client

        async for event in self.trigger.run():
            self.assertEqual(event, {"status": "success", "message": "Job run 123 has completed successfully.", "job_id": "123"})

    @patch('asyncio.sleep', return_value=None)
    @patch('ray.dashboard.modules.job.sdk.JobSubmissionClient')
    async def test_job_error_on_exception(self, mock_client_class, mock_sleep):
        mock_client = MagicMock()
        mock_client.get_job_status.side_effect = Exception("Connection error")
        mock_client_class.return_value = mock_client

        async for event in self.trigger.run():
            self.assertEqual(event, {"status": "error", "message": "Connection error", "job_id": "123"})

    @patch('time.time', side_effect=[100, 200, 300, 400, 10000])  # Simulating time passing and timeout
    @patch('asyncio.sleep', return_value=None)
    @patch('ray.dashboard.modules.job.sdk.JobSubmissionClient')
    async def test_timeout_reached_before_job_ends(self, mock_client_class, mock_sleep, mock_time):
        mock_client = MagicMock()
        mock_client.get_job_status.return_value = JobStatus.RUNNING
        mock_client_class.return_value = mock_client

        async for event in self.trigger.run():
            self.assertEqual(event, {
                "status": "error",
                "message": "Job run 123 has not reached a terminal status after 10000 seconds.",
                "job_id": "123",
            })

    @patch('asyncio.sleep', return_value=None)
    @patch('ray.dashboard.modules.job.sdk.JobSubmissionClient')
    async def test_job_stopped(self, mock_client_class, mock_sleep):
        mock_client = MagicMock()
        mock_client.get_job_status.return_value = JobStatus.STOPPED
        mock_client_class.return_value = mock_client

        async for event in self.trigger.run():
            self.assertEqual(event, {
                "status": "cancelled",
                "message": "Job run 123 has been stopped.",
                "job_id": "123",
            })

if __name__ == '__main__':
    unittest.main()
