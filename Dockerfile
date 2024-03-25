FROM quay.io/astronomer/astro-runtime:10.5.0

RUN curl --silent --location "https://github.com/weaveworks/eksctl/releases/latest/download/eksctl_$(uname -s)_amd64.tar.gz" | tar xz -C /tmp

USER root

RUN mv /tmp/eksctl /usr/local/bin
