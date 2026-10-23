"""Shared DockerOperator builder for OpenMetadata ingestion workflows.

Same Docker-outside-of-Docker pattern as bsis_pipeline_helpers.py's
spark_job_operator -- a one-off container from a dedicated, minimal image
(openmetadata-ingestion/Dockerfile) runs `metadata ingest -c <config.yaml>`,
triggered by our own Airflow. Deliberately NOT using OpenMetadata's own
bundled ingestion-Airflow container -- one orchestrator for this whole
lakehouse, not two.
"""

from __future__ import annotations

import os

OM_INGESTION_IMAGE = "bot_lakehouse-openmetadata-ingestion"
DOCKER_NETWORK = "bot_lakehouse_default"

_REQUIRED_ENV_VARS = ("OM_INGESTION_BOT_TOKEN",)


def _om_container_environment() -> dict[str, str]:
    missing = [v for v in _REQUIRED_ENV_VARS if not os.environ.get(v)]
    if missing:
        raise RuntimeError(
            f"Missing required env var(s) for OpenMetadata ingestion: {', '.join(missing)}. "
            "OM_INGESTION_BOT_TOKEN comes from the OpenMetadata UI (after first "
            "login: Settings -> Bots -> ingestion-bot -> token) -- it can't be "
            "generated ahead of time, set it in .env once you have it."
        )
    return {
        "OM_INGESTION_BOT_TOKEN": os.environ["OM_INGESTION_BOT_TOKEN"],
        "OM_SERVER_HOST_PORT": os.environ.get("OM_SERVER_HOST_PORT", "http://om-server:8585/api"),
        "TRINO_USERNAME": os.environ.get("TRINO_USERNAME", "lakehouse_ingestion"),
    }


def _host_project_dir() -> str:
    host_dir = os.environ.get("HOST_PROJECT_DIR")
    if not host_dir:
        raise RuntimeError("HOST_PROJECT_DIR is not set -- see .env.")
    return host_dir.rstrip("\\/")


def om_ingest_operator(task_id: str, config_file: str):
    """A DockerOperator that runs `metadata ingest -c <config_file>` against
    one of the workflow configs in openmetadata-ingestion/connectors/.
    """
    from docker.types import Mount

    from airflow.providers.docker.operators.docker import DockerOperator

    host_dir = _host_project_dir()
    return DockerOperator(
        task_id=task_id,
        image=OM_INGESTION_IMAGE,
        command=["metadata", "ingest", "-c", f"/opt/ingestion/connectors/{config_file}"],
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        environment=_om_container_environment(),
        mounts=[
            Mount(
                source=f"{host_dir}/openmetadata-ingestion/connectors",
                target="/opt/ingestion/connectors",
                type="bind",
            ),
        ],
        mount_tmp_dir=False,
        auto_remove="success",
        do_xcom_push=False,
    )


def _om_script_operator(task_id: str, script_name: str):
    """A DockerOperator that runs `python /opt/ingestion/<script_name>` --
    shared by every openmetadata-ingestion/*.py script that (like
    set_governance.py and set_lineage.py) needs spark/config +
    source_config.py mounted too, since they're driven by the exact same
    per-directorate/source-system YAML the Spark jobs use, not a second copy
    of the directorate/domain/dataset mapping.
    """
    from docker.types import Mount

    from airflow.providers.docker.operators.docker import DockerOperator

    host_dir = _host_project_dir()
    return DockerOperator(
        task_id=task_id,
        image=OM_INGESTION_IMAGE,
        command=["python", f"/opt/ingestion/{script_name}"],
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        environment=_om_container_environment(),
        mounts=[
            Mount(
                source=f"{host_dir}/openmetadata-ingestion/{script_name}",
                target=f"/opt/ingestion/{script_name}",
                type="bind",
            ),
            Mount(
                source=f"{host_dir}/spark/jobs/source_config.py",
                target="/opt/spark-jobs/source_config.py",
                type="bind",
                read_only=True,
            ),
            Mount(
                source=f"{host_dir}/spark/config",
                target="/opt/spark-config",
                type="bind",
                read_only=True,
            ),
        ],
        mount_tmp_dir=False,
        auto_remove="success",
        do_xcom_push=False,
    )


def om_governance_operator(task_id: str = "enrich_governance"):
    """Sets owner (directorate, as an OpenMetadata Team) and domain on every
    table the metadata-ingest tasks created -- see set_governance.py.
    """
    return _om_script_operator(task_id, "set_governance.py")


def om_lineage_operator(task_id: str = "enrich_lineage"):
    """Draws Bronze->Silver->Gold lineage edges -- see set_lineage.py."""
    return _om_script_operator(task_id, "set_lineage.py")
