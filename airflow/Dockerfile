FROM apache/airflow:2.10.5-python3.11

USER root

RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    unixodbc \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64,arm64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
    && rm -rf /var/lib/apt/lists/*
#so we can run the valid sql script
RUN apt-get update \
    && ACCEPT_EULA=Y apt-get install -y mssql-tools18 \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="$PATH:/opt/mssql-tools18/bin"

USER airflow

COPY python/requirements.txt /requirements.txt
#here these are the requireemnts that are needed for the scripts of python to run so airflow needs them so they can run

RUN pip install --no-cache-dir \
    --default-timeout=120 \
    --retries 5 \
    "apache-airflow==2.10.5" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt" \
    -r /requirements.txt
#so we have the official image + some dependencies
