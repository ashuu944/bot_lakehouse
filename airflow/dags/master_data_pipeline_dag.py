"""DFSS/BSIS master/reference data: Bronze land -> Silver clean.

Covers the 11 master/reference datasets (institutions, sectors, risk_types,
...) -- see spark/jobs/silver_master_transform.py's MASTER_DATASETS. Unlike
the daily/monthly pipelines, this isn't a recurring time-series extract: a
run just reflects master_data_ingestion.py's current MASTER_DATA_PATH CSVs
(spark/master_data/DFSS/BSIS/*.csv) as of whenever it's triggered, same
"reference, overwritten each run" semantics those two jobs already have.

schedule=None, same reasoning as dfss_bsis_backfill: trigger this by hand
(UI "Trigger DAG" or CLI) whenever those CSVs actually change, not on a
fixed cadence.

No load-to-Neo4j (or any other downstream store) step here, same as the
daily/monthly/backfill pipelines -- this pipeline's job ends at Silver.

Usage:
    airflow dags trigger dfss_bsis_master_data
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG

from bsis_pipeline_helpers import master_data_ingest_operator, spark_job_operator

TZ = "Africa/Dar_es_Salaam"

default_args = {
    "owner": "dfss-bsis",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dfss_bsis_master_data",
    description="DFSS/BSIS: land master/reference CSVs into Bronze, clean into Silver (manual trigger only)",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz=TZ),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["dfss", "bsis", "master", "bronze", "silver"],
) as dag:

    ingest_master_bronze = master_data_ingest_operator()

    transform_master_silver = spark_job_operator(
        task_id="transform_master_silver",
        job_script="silver_master_transform.py",
        arguments=[],
    )

    ingest_master_bronze >> transform_master_silver
