from __future__ import annotations
import asyncio
from typing import AsyncIterator
from ray.job_submission import JobSubmissionClient, JobStatus
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.exceptions import AirflowException

import logging
logger = logging.getLogger("RayJobTrigger")
logger.setLevel(logging.INFO)

class RayJobTrigger(BaseTrigger):
    def __init__(self, job_id: str, url: str, timeout: int = 600, poll_interval: int = 30):
        super().__init__()
        self.job_id = job_id
        self.url = url
        self.timeout = timeout
        self.poll_interval = poll_interval
        self.status_to_wait_for = {JobStatus.SUCCEEDED, JobStatus.STOPPED, JobStatus.FAILED}

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
        
        logger.info(f"Polling for {self.job_id} every {self.poll_interval} seconds...")

        client = JobSubmissionClient(f"http://{self.url}")

        start_time = asyncio.get_running_loop().time()
        while True:
            status = client.get_job_status(self.job_id)

            logger.info(f"Job id {self.job_id} is {status}")

            if status in self.status_to_wait_for:
                logger.info(f"Job {self.job_id} status: {status}")
                logs = client.get_job_logs(self.job_id)
                logger.info(f"Final logs for job {self.job_id}: {logs}")
                yield TriggerEvent({"status": status, "job_id": self.job_id})
                break

            if (asyncio.get_running_loop().time() - start_time) > self.timeout:
                logger.info(f"Job {self.job_id} timed out.")
                yield TriggerEvent({"status": "timeout", "job_id": self.job_id})
                break

            # Fetch and log intermediate logs at each polling interval
            logs = client.get_job_logs(self.job_id)
            logger.info(f"Logs for job {self.job_id}: {logs}")

            await asyncio.sleep(self.poll_interval)
