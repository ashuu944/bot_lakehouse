# Dedicated, minimal image for running OpenMetadata ingestion workflows
# (metadata ingest -c <config.yaml>) as one-off containers triggered by our
# own Airflow -- same Docker-outside-of-Docker pattern as the Spark jobs
# (see airflow/dags/om_ingestion_helpers.py), not bolted onto the Spark image,
# which has nothing to do with this workload.
FROM python:3.10-slim

# Same corporate SSL-inspecting proxy (Fortinet) spark/Dockerfile and
# airflow/Dockerfile already work around.
COPY spark/certs/fortinet-ca-1.pem /usr/local/share/ca-certificates/fortinet-ca-1.crt
COPY spark/certs/fortinet-ca-2.pem /usr/local/share/ca-certificates/fortinet-ca-2.crt
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV PIP_CERT=/etc/ssl/certs/ca-certificates.crt
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

# Pinned to the exact OpenMetadata server version (2.0.5, see docker-compose.yml) --
# mixing ingestion-framework and server versions is unsupported upstream.
# Note: the extra is "datalake-s3" (S3-compatible, which MinIO is), not a
# generic "datalake" -- that name doesn't exist in this package (confirmed via
# `importlib.metadata`: only datalake-s3/datalake-gcs/datalake-azure are
# declared). Using the wrong name fails silently at install time (pip just
# ignores an unknown extra) and only surfaces later as a confusing
# "ModuleNotFoundError: No module named 'pandas'" buried under a generic
# "missing plugin [datalake]" message at ingestion time.
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --retries 10 --timeout 120 \
    "openmetadata-ingestion[trino,datalake-s3]==2.0.5"

WORKDIR /opt/ingestion
