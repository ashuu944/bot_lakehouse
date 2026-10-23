# Adds the Docker provider so DAGs can launch one-off Spark containers via
# DockerOperator (Docker-outside-of-Docker: the Airflow scheduler/webserver
# talks to the HOST's Docker daemon through the socket mounted in
# docker-compose.yml, then runs `spark-submit` in a fresh container from the
# same `bot_lakehouse-spark` image already used for `docker compose exec
# spark ...`). This is Phase 5 from the project README.
#
# Installed at BUILD time, pinned against Airflow's own constraints file for
# this exact Airflow+Python combination -- NOT via _PIP_ADDITIONAL_REQUIREMENTS
# at container start, which Airflow's own docs call out as fine for a quick
# local try, not for anything meant to keep running. The constraints file
# resolves a provider version Airflow 2.10.3/Python 3.11 is actually tested
# against, instead of us guessing and hardcoding a version number here.
FROM apache/airflow:2.10.3-python3.11

USER root

# Same corporate SSL-inspecting proxy (Fortinet) spark/Dockerfile already
# works around -- without this, pip can't reach PyPI/GitHub to install
# anything. update-ca-certificates covers curl/apt; pip vendors its own
# certifi bundle instead of the system store, so it needs pointing at the
# updated system bundle explicitly via these env vars.
COPY spark/certs/fortinet-ca-1.pem /usr/local/share/ca-certificates/fortinet-ca-1.crt
COPY spark/certs/fortinet-ca-2.pem /usr/local/share/ca-certificates/fortinet-ca-2.crt
RUN update-ca-certificates

ENV PIP_CERT=/etc/ssl/certs/ca-certificates.crt
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

USER airflow
RUN pip install --no-cache-dir --retries 10 --timeout 120 \
    "apache-airflow-providers-docker" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.11.txt"
