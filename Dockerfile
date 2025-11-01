FROM apache/airflow:2.10.5

# Switch to root to install system packages
USER root

# Install jq and curl (if not already in base image)
RUN apt-get update && \
    apt-get install -y --no-install-recommends jq curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

COPY requirements.txt /tmp/

RUN --mount=type=cache,target=/root/.cache/pip pip install --no-cache-dir -r /tmp/requirements.txt