from airflow.models import BaseOperator, BaseOperatorLink, XCom
import yaml


class RayClusterExecutor(BaseOperator):
    def __init__(self,
                 k8Config: str = None,
                 cluster_name: str = None,
                 ray_cluster_spec : dict = None,
                 ray_cluster_yaml : str = None,
                 delete_cluster: bool = None):
        pass

    def execute(self):
        pass


class SubmitRayJob(BaseOperator):
    def __init__(self):
        pass

    def execute(self):
        pass


class DeployRayService(BaseOperator):
    def __init__(self):
        pass

    def execute(self):
        pass