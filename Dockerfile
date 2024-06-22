FROM quay.io/astronomer/astro-runtime:11.3.0

RUN pip install --user astro_provider_ray-1.0.0-py2.py3-none-any.whl

USER root

# Install eksctl
RUN curl --silent --location "https://github.com/weaveworks/eksctl/releases/latest/download/eksctl_$(uname -s)_amd64.tar.gz" | tar xz -C /tmp && \
    mv /tmp/eksctl /usr/local/bin

# Install Helm
RUN curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 -o get_helm.sh && \
    chmod 700 get_helm.sh && \
    ./get_helm.sh






