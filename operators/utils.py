from kubernetes import client, config, watch
import yaml
import time

def create_service_and_get_url(namespace="default", yaml_file="ray-head-service.yaml"):
    """
    Creates a service in Kubernetes from a YAML file and retrieves its external URL.

    :param namespace: The Kubernetes namespace where the service is to be created.
    :param yaml_file: Path to the YAML file with the service definition.
    """
    config.load_kube_config()

    with open(yaml_file) as f:
        service_data = yaml.safe_load(f)
    
    v1 = client.CoreV1Api()
    created_service = v1.create_namespaced_service(namespace=namespace, body=service_data)
    print(f"Service {created_service.metadata.name} created. Waiting for an external IP...")

    w = watch.Watch()
    for event in w.stream(v1.list_namespaced_service, namespace=namespace, timeout_seconds=600):
        service = event['object']
        if service.metadata.name == created_service.metadata.name and service.status.load_balancer.ingress:
            external_ip = service.status.load_balancer.ingress[0].ip
            port = service.spec.ports[0].port
            url = f"http://{external_ip}:{port}"
            print(f"Service URL: {url}")
            w.stop()