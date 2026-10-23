"""DFSS/BSIS monthly pipeline: Bronze extract -> Silver transform.

Covers the 9 monthly-cadence BSIS datasets (see bsis_frequency.MONTHLY_DATASETS):
lcr, nsfr, capital_adequacy, sensitivity_market_risk, asset_quality,
credit_exposure, balance_sheet, earnings, income_statement.

No load-to-Neo4j (or any other downstream store) step here -- this pipeline's
job ends at Silver, by design; see bsis_pipeline_helpers.py's module docstring.

Monthly BSIS reports are period-end reports (e.g. 2026-09-30), not "whatever
date the DAG happened to run on" -- `resolve_period_end` below computes the
last day of the month that just closed from the DAG run's own data interval,
so --date is always correct regardless of which day of the month this runs.

catchup is deliberately False -- see daily_bsis_pipeline_dag.py's docstring.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from bsis_pipeline_helpers import MONTHLY_DATASETS, spark_job_operator

TZ = "Africa/Dar_es_Salaam"
MONTHLY_DATASET_ARG = ",".join(sorted(MONTHLY_DATASETS))

default_args = {
    "owner": "dfss-bsis",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15),
}


def _resolve_period_end(data_interval_end, **context) -> str:
    """Last day of the month that just closed, as of this run's data interval
    -- e.g. a run whose interval ends 2026-11-05 resolves to 2026-10-31,
    regardless of which day of the month the schedule actually fires on.
    """
    period_end = data_interval_end.in_timezone(TZ).start_of("month").subtract(days=1)
    return period_end.to_date_string()


with DAG(
    dag_id="dfss_bsis_monthly_pipeline",
    description="DFSS/BSIS: extract monthly fact datasets into Bronze, transform into Silver",
    # 5th of the month at 07:00 EAT -- a few days' grace after month-end for
    # institutions to finish submitting, before BOT's own extraction runs.
    # Revisit if DFSS confirms a different grace window.
    schedule="0 7 5 * *",
    start_date=pendulum.datetime(2026, 10, 1, tz=TZ),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["dfss", "bsis", "monthly", "bronze", "silver"],
) as dag:

    resolve_period_end = PythonOperator(
        task_id="resolve_period_end",
        python_callable=_resolve_period_end,
    )

    extract_monthly = spark_job_operator(
        task_id="extract_monthly_sources",
        job_script="bronze_ingestion.py",
        arguments=[
            "--dataset", MONTHLY_DATASET_ARG,
            "--date", "{{ ti.xcom_pull(task_ids='resolve_period_end') }}",
        ],
    )

    transform_monthly = spark_job_operator(
        task_id="transform_monthly_sources",
        job_script="silver_transform.py",
        arguments=["--dataset", MONTHLY_DATASET_ARG],
    )

    resolve_period_end >> extract_monthly >> transform_monthly
