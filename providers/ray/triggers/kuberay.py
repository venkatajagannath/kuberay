from __future__ import annotations
import asyncio
from typing import Any, AsyncIterator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.exceptions import AirflowException
from ray.dashboard.modules.job.sdk import JobSubmissionClient, JobStatus
import logging

logger = logging.getLogger("airflow.task")

class RayJobTrigger(BaseTrigger):
    def __init__(self,
                 job_id: str,
                 url: str,
                 timeout: int = 600,
                 poll_interval: int = 30):
        super().__init__()
        self.job_id = job_id
        self.url = url
        self.timeout = timeout
        self.poll_interval = poll_interval
        # Note: Assuming JobStatus contains the required status attributes
        self.final_statuses = {JobStatus.SUCCEEDED, JobStatus.STOPPED, JobStatus.FAILED}

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return ("providers.ray.triggers.kuberay.RayJobTrigger", {
            "job_id": self.job_id,
            "url": self.url,
            "timeout": self.timeout,
            "poll_interval": self.poll_interval
        })

    async def run(self) -> AsyncIterator[TriggerEvent]:
        if not self.job_id:
            raise AirflowException("Job_id is not provided")

        logger.info(f"Polling for job {self.job_id} every {self.poll_interval} seconds...")
        client = JobSubmissionClient(f"http://{self.url}")

        start_time = asyncio.get_running_loop().time()
        while True:
            current_status = client.get_job_status(self.job_id).status
            logger.info(f"Job id {self.job_id} status: {current_status}")

            if current_status in self.final_statuses:
                logger.info(f"Final status for job {self.job_id}: {current_status}")
                logs = client.get_job_logs(self.job_id)
                logger.info(f"Final logs for job {self.job_id}: {logs}")
                yield TriggerEvent({"status": current_status.value, "job_id": self.job_id})
                break

            if (asyncio.get_running_loop().time() - start_time) > self.timeout:
                logger.warning(f"Job {self.job_id} timed out after {self.timeout} seconds.")
                yield TriggerEvent({"status": "timeout", "job_id": self.job_id})
                break

            # Optionally, fetch and log intermediate logs at each polling interval.
            # Commented out to prevent log spamming. Uncomment if needed.
            logs = client.get_job_logs(self.job_id)
            logger.debug(f"Interim logs for job {self.job_id}: {logs}")

            await asyncio.sleep(self.poll_interval)

