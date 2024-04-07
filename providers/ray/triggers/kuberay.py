from __future__ import annotations
import asyncio
from typing import Any, AsyncIterator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.exceptions import AirflowException
from ray.dashboard.modules.job.sdk import JobSubmissionClient, JobStatus
import logging
import time

logger = logging.getLogger("kuberay.py")

class RayJobTrigger(BaseTrigger):
    def __init__(self,
                 job_id: str,
                 url: str,
                 end_time: float,
                 poll_interval: int = 30):
        super().__init__()
        self.job_id = job_id
        self.url = url
        self.end_time = end_time
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return ("providers.ray.triggers.kuberay.RayJobTrigger", {
            "job_id": self.job_id,
            "url": self.url,
            "end_time": self.end_time,
            "poll_interval": self.poll_interval
        })

    async def run(self) -> AsyncIterator[TriggerEvent]:
        if not self.job_id:
            yield TriggerEvent({"status": "error", "message": "No job_id provided to async trigger", "job_id": self.job_id})

        try:
            logger.info(f"Polling for job {self.job_id} every {self.poll_interval} seconds...")
            client = JobSubmissionClient(f"{self.url}")

            while await self.get_current_status(client=client):
                if self.end_time < time.time():
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "message": f"Job run {self.job_id} has not reached a terminal status after "
                            f"{self.end_time} seconds.",
                            "job_id": self.job_id,
                        }
                    )
                    return
                await asyncio.sleep(self.poll_interval)
            
            completed_status = await client.get_job_status(self.job_id)
            if completed_status == JobStatus.SUCCEEDED:
                yield TriggerEvent(
                    {
                        "status": "success",
                        "message": f"Job run {self.job_id} has completed successfully.",
                        "job_id": self.job_id,
                    }
                )
            elif completed_status == JobStatus.STOPPED:
                yield TriggerEvent(
                    {
                        "status": "cancelled",
                        "message": f"Job run {self.job_id} has been stopped.",
                        "job_id": self.job_id,
                    }
                )
            else:
                yield TriggerEvent(
                    {
                        "status": "error",
                        "message": f"Job run {self.job_id} has failed.",
                        "job_id": self.job_id,
                    }
                )
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e), "job_id": self.job_id})
        
    async def get_current_status(self, client: JobSubmissionClient) -> bool:

        job_status = await client.get_job_status(self.job_id)
        logging.info(f"Current job status for {self.job_id} is: {job_status}")
        if job_status in (JobStatus.RUNNING,JobStatus.PENDING):
            return True
        else:
            return False


