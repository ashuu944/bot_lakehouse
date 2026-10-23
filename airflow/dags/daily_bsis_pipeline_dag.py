"""DFSS/BSIS daily pipeline: Bronze extract -> Silver transform.

Covers the 3 daily-cadence BSIS datasets (see bsis_frequency.DAILY_DATASETS):
smr_daily_compliance, liquidity_compliance, net_open_position.

No load-to-Neo4j (or any other downstream store) step here -- this pipeline's
job ends at Silver, by design; see bsis_pipeline_helpers.py's module docstring.

catchup is deliberately False: the historical backfill already happened once,
by hand, per the README (`bronze_ingestion.py --start ...`). This DAG is for
steady-state daily runs going forward, not for silently replaying months of
history the first time it's turned on.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG

from bsis_pipeline_helpers import DAILY_DATASETS, spark_job_operator

TZ = "Africa/Dar_es_Salaam"
DAILY_DATASET_ARG = ",".join(sorted(DAILY_DATASETS))

default_args = {
    "owner": "dfss-bsis",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dfss_bsis_daily_pipeline",
    description="DFSS/BSIS: extract daily fact datasets into Bronze, transform into Silver",
    # Daily reports for date D settle overnight; a 06:00 EAT run gives D-1 a
    # full day to land in BSIS before extraction, and leaves the working day
    # free to react to a failure -- same reasoning the existing BOT risk
    # pipeline (bank_profiling_risk_analysis) uses for its own daily DAG.
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz=TZ),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["dfss", "bsis", "daily", "bronze", "silver"],
) as dag:

    extract_daily = spark_job_operator(
        task_id="extract_daily_sources",
        job_script="bronze_ingestion.py",
        arguments=["--dataset", DAILY_DATASET_ARG, "--date", "{{ ds }}"],
    )

    transform_daily = spark_job_operator(
        task_id="transform_daily_sources",
        job_script="silver_transform.py",
        arguments=["--dataset", DAILY_DATASET_ARG],
    )

    extract_daily >> transform_daily
