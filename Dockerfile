FROM quay.io/astronomer/astro-runtime:10.5.0

# Set the architecture
#ARG ARCH=amd64

# Assuming a Linux environment for the Docker image
#ENV PLATFORM=Linux_${ARCH}

# Install eksctl
#RUN curl -sLO "https://github.com/eksctl-io/eksctl/releases/latest/download/eksctl_$PLATFORM.tar.gz" && \
#    curl -sL "https://github.com/eksctl-io/eksctl/releases/latest/download/eksctl_checksums.txt" | grep $PLATFORM | sha256sum --check && \
#    tar -xzf eksctl_$PLATFORM.tar.gz -C /tmp && \
#    rm eksctl_$PLATFORM.tar.gz && \
#    mv /tmp/eksctl /usr/local/bin/eksctl
