from airflow.triggers.base import BaseTrigger, TriggerEvent
import asyncio

class RayJobTrigger(BaseTrigger):
    def __init__(self, job_id, poll_interval=30):
        super().__init__()
        self.job_id = job_id
        self.poll_interval = poll_interval

    def serialize(self):
        return ("providers.ray.triggers.kuberay.RayJobTrigger", {"job_id": self.job_id, "poll_interval": self.poll_interval})

    async def run(self):
        while True:
            # Check if the job is complete
            if LongRunningJobAPI.is_job_complete(self.job_id):
                # Job is complete, emit a TriggerEvent and exit
                return TriggerEvent({"status": "complete", "job_id": self.job_id})
            else:
                # Wait for `poll_interval` seconds before checking again
                await asyncio.sleep(self.poll_interval)
