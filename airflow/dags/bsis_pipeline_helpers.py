"""Shared DockerOperator builder for the DFSS/BSIS pipeline DAGs.

Runs bot_lakehouse's existing Spark jobs (bronze_ingestion.py, silver_transform.py)
as one-off containers from the `bot_lakehouse-spark` image -- Docker-outside-of-
Docker, the Airflow container talks to the HOST's Docker daemon through the
mounted socket -- instead of reimplementing extract/transform logic here. This
mirrors `docker compose exec spark spark-submit ...` exactly, just triggered by
Airflow instead of a human, per the README's Phase 5 plan.

No Neo4j/load step here by design: this pipeline's job ends at Silver. Loading
Gold into Neo4j (or any other downstream store) is a separate, later concern
that reads from Gold, not a task bolted onto ingestion -- see the architecture
discussion this was built from.
"""

from __future__ import annotations

import os
import sys

# Mounted read-only into this container (docker-compose.yml) so the daily/
# monthly dataset split has exactly one source of truth -- the same module
# bronze_ingestion.py itself imports -- instead of a second, driftable copy
# of that list living in the Airflow DAGs.
_SPARK_JOBS_DIR = "/opt/spark-jobs"
if _SPARK_JOBS_DIR not in sys.path:
    sys.path.insert(0, _SPARK_JOBS_DIR)

from bsis_frequency import DAILY_DATASETS, MONTHLY_DATASETS  # noqa: E402

SPARK_IMAGE = "bot_lakehouse-spark"
DOCKER_NETWORK = "bot_lakehouse_default"

# Env vars the spawned Spark container needs. DockerOperator starts an
# independent container, not the running `spark` service, so it does not
# inherit that service's environment block -- these must be forwarded
# explicitly from this (the Airflow) container's own environment, which
# docker-compose.yml populates from .env.
_REQUIRED_ENV_VARS = (
    "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "MINIO_ENDPOINT", "MINIO_BUCKET",
    "DB_USER", "DB_PASSWORD", "DB_DSN", "BSIS_ENV",
)


def _spark_container_environment() -> dict[str, str]:
    missing = [v for v in _REQUIRED_ENV_VARS if not os.environ.get(v)]
    if missing:
        raise RuntimeError(
            f"Missing required env var(s) for the Spark container: {', '.join(missing)}. "
            "Set them in bot_lakehouse/.env -- the airflow service in "
            "docker-compose.yml forwards them from there."
        )
    return {v: os.environ[v] for v in _REQUIRED_ENV_VARS}


def _host_project_dir() -> str:
    host_dir = os.environ.get("HOST_PROJECT_DIR")
    if not host_dir:
        raise RuntimeError(
            "HOST_PROJECT_DIR is not set. DockerOperator talks to the HOST's "
            "Docker daemon (via the mounted socket), so the volumes it mounts "
            "must be host paths, not paths inside this container -- set "
            "HOST_PROJECT_DIR in .env to this project's absolute path on the "
            "host running Docker."
        )
    return host_dir.rstrip("\\/")


def spark_job_operator(task_id: str, job_script: str, arguments: list[str]):
    """A DockerOperator that runs `spark-submit /opt/spark-jobs/<job_script> <arguments>`
    in a fresh container from the image the `spark`/`spark-thriftserver`
    services already use, on the same Docker network as the rest of the stack.
    """
    from docker.types import Mount

    from airflow.providers.docker.operators.docker import DockerOperator

    host_dir = _host_project_dir()
    return DockerOperator(
        task_id=task_id,
        image=SPARK_IMAGE,
        command=["spark-submit", f"/opt/spark-jobs/{job_script}", *arguments],
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        environment=_spark_container_environment(),
        mounts=[
            Mount(source=f"{host_dir}/spark/jobs", target="/opt/spark-jobs", type="bind"),
            # Per-directorate/source-system dataset configs (source_config.py) --
            # bronze_ingestion.py/silver_transform.py need this the same way
            # the `spark` compose service does (docker-compose.yml).
            Mount(source=f"{host_dir}/spark/config", target="/opt/spark-config", type="bind", read_only=True),
        ],
        mount_tmp_dir=False,
        auto_remove="success",
        do_xcom_push=False,
    )
