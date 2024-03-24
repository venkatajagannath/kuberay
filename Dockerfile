FROM quay.io/astronomer/astro-runtime:10.5.0

# Install eksctl
RUN curl --silent --location "https://github.com/weaveworks/eksctl/releases/download/0.75.0/eksctl_$(uname -s)_amd64.tar.gz" | tar xz -C /tmp \
    && mv /tmp/eksctl /usr/local/bin
