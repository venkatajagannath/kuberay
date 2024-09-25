FROM quay.io/astronomer/astro-runtime:12.1.1

# Install astro_provider_ray
#RUN pip install --user astro_provider_ray-0.2.1-py3-none-any.whl

USER root

ENV AWS_ACCESS_KEY_ID="ASIA3HGKZAEPRFKYNMRW"
ENV AWS_SECRET_ACCESS_KEY="lrhmc+3A2jDf6mO4QhOSxUk3QNvo2w3VxjgRL48B"
ENV AWS_SESSION_TOKEN="IQoJb3JpZ2luX2VjELn//////////wEaCXVzLWVhc3QtMiJIMEYCIQCKsVpwMkih2LGg7gzbckOHFBSfP+mocfrjXtmoIVvTEQIhANgjgSXI1DxADdaKuMybhoaOXMXBE1qnqOXZQAvq+qwBKr4DCPP//////////wEQABoMNzcxMzcxODkzMDIzIgz3yDE0X+W2hI7lhz8qkgPhCb9EYUnWAOxMlWWPJRP79dvjqK2UwF9GfU8zidOSWsAXyQII53pL0ujS2+m/fuq3veiWSwQCDrEE7fFkxIt0B53lQyFRCo2m7stMVavmq+2cSdipRXJGA5PReZcC2Wq8o5AnHEMva06p4cSYVsLk0gdFkvuLhcfFmXplPyPps9nHQTDZWlrPIvmMDOqMDCMf4zoe/PMMUn8m/G0iMMTGtNox1BNoXdvKe8W/fSluteS4T3ieBt/80XhkDO66zlOfc/F/VVq4rWwoRZE57Ynp8Lf/ubMlpuFlsh4CO423OZ5HButwuXekQ4HCq/2hBPj0XJiiunrgtFW5elBBhoBLwj6W082c6gSXtrlpEMQIbWgxGVESPofPYMtJZKc8K0M+OfX+8t3BRkW3/XyGxyOPeryN5lZyGxYNGcGNeZBwOC1ijkMS7246hYh+zilWluhYZS/4ehstLISUZSssWMj8qRMGtC2jH76VNYugFKOiuj0GpocrcZnqwb+jGgWs/sINYW3JenJqcLEekWqCf70K2z0wuY3RtwY6pQH9xQqLx/Y5ECeTqaKtcQmMrTeEBSIswjggLNplfcG9yiTpKmd1dcsNfOnGuCkvnRjvnGVPrI28weJwHjWwZL/8WDjZ3BfI/2zMTfMjCvD05Vs8JeL7wQ1b6KgzBLXFH18GNsEdnmdXv6s5chw3np6rE8zHFTSeH4viAkZQGg/HdbJ/Dx8LORS7cVr3l6hTmwTuPWMo4HqnCimhGnfzig8GkPobcvA="

# Install Helm
RUN apt-get update && apt-get install -y unzip
RUN curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 -o get_helm.sh && \
    chmod 700 get_helm.sh && \
    ./get_helm.sh

# Clean up
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

#ENV AZURE_TENANT_ID="b84efac8-cfee-467a-b223-23b9aea1486d"
#ENV AZURE_CLIENT_ID="37db3e14-fa81-45fd-8518-afe387e614d0"
#ENV AZURE_CLIENT_SECRET="5cr8Q~~F0X~sh6Lz_2oHjkEgZ1dNyCPLzJ6Umasn"

#ENV GOOGLE_CLOUD_PROJECT="astronomer-cloud"

# Install gcloud CLI
#RUN apt-get update && apt-get install -y curl gnupg
#RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
#RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
#RUN apt-get update && apt-get install -y google-cloud-sdk

# Install gke-gcloud-auth-plugin and kubectl
#RUN apt-get install -y google-cloud-cli-gke-gcloud-auth-plugin kubectl

#ENV GOOGLE_APPLICATION_CREDENTIALS="/usr/local/airflow/dags/scripts/key.json"
#COPY path/to/your/key.json /usr/local/airflow/dags/scripts/key.json

# Activate service account and set project
#RUN gcloud auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS
#RUN gcloud config set project astronomer-cloud

# Switch back to the airflow user
USER astro