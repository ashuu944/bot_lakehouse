"""DFSS/BSIS backfill: on-demand historical extract -> transform, triggered
manually with a date range (and optionally a frequency/dataset selection).

Unlike bank_profiling_risk_analysis's run_backfill_pipeline.py, this DAG does
NOT loop per-date in Python with its own checkpoint/retry bookkeeping --
bronze_ingestion.py already walks a date range internally, one dataset at a
time, at its own correct cadence (see bsis_frequency.generate_date_range), so
a single --start/--end call already covers the whole range. Reproducing a
per-date loop here would duplicate logic that already exists and works.

Trigger this from the Airflow UI ("Trigger DAG w/ config") or CLI:

    airflow dags trigger dfss_bsis_backfill --conf '{
        "frequency": "monthly",
        "start_date": "2026-01-01",
        "end_date": "2026-06-30"
    }'

    # or specific datasets, overriding the frequency selection entirely:
    airflow dags trigger dfss_bsis_backfill --conf '{
        "datasets": "lcr,nsfr",
        "start_date": "2026-01-01"
    }'

No load-to-Neo4j (or any other downstream store) step here, same as the
daily/monthly pipelines -- this pipeline's job ends at Silver.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from bsis_pipeline_helpers import DAILY_DATASETS, MONTHLY_DATASETS, spark_job_operator

TZ = "Africa/Dar_es_Salaam"
_ALL_DATASETS = DAILY_DATASETS | MONTHLY_DATASETS

default_args = {
    "owner": "dfss-bsis",
    "depends_on_past": False,
    "retries": 1,  # a backfill is already a deliberate, watched, one-off run
    "retry_delay": timedelta(minutes=10),
}


def _resolve_backfill_scope(**context) -> None:
    """Turn the trigger-time params into a concrete dataset list + date range,
    validated against bsis_frequency's known datasets rather than trusting
    free-text input straight into a CLI arg.
    """
    params = context["params"]
    frequency = params["frequency"]
    datasets_override = (params.get("datasets") or "").strip()
    start_date = params["start_date"]
    end_date = (params.get("end_date") or "").strip() or pendulum.now(TZ).to_date_string()

    if datasets_override:
        requested = [d.strip() for d in datasets_override.split(",") if d.strip()]
        unknown = [d for d in requested if d not in _ALL_DATASETS]
        if unknown:
            raise AirflowException(
                f"Unknown dataset(s) in 'datasets' param: {', '.join(unknown)}. "
                f"Valid datasets: {', '.join(sorted(_ALL_DATASETS))}"
            )
        dataset_list = requested
    elif frequency == "daily":
        dataset_list = sorted(DAILY_DATASETS)
    elif frequency == "monthly":
        dataset_list = sorted(MONTHLY_DATASETS)
    elif frequency == "all":
        dataset_list = sorted(_ALL_DATASETS)
    else:
        raise AirflowException(
            f"Unknown 'frequency' param: {frequency!r}. Expected 'all', 'daily', or 'monthly'."
        )

    ti = context["ti"]
    ti.xcom_push(key="dataset_arg", value=",".join(dataset_list))
    ti.xcom_push(key="start_date", value=start_date)
    ti.xcom_push(key="end_date", value=end_date)


with DAG(
    dag_id="dfss_bsis_backfill",
    description="DFSS/BSIS: on-demand historical extract + transform for a date range (manual trigger only)",
    schedule=None,  # on-demand utility, not a recurring pipeline
    start_date=pendulum.datetime(2026, 10, 1, tz=TZ),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["dfss", "bsis", "backfill", "bronze", "silver"],
    params={
        "frequency": Param(
            "all", enum=["all", "daily", "monthly"],
            description="Which cadence to backfill. Ignored if 'datasets' is set.",
        ),
        "datasets": Param(
            "", type=["string", "null"],
            description="Comma-separated dataset keys, overrides 'frequency' entirely (e.g. 'lcr,nsfr'). Leave blank to use 'frequency'.",
        ),
        "start_date": Param(
            "2026-06-01", type="string",
            description="Backfill start date, YYYY-MM-DD (inclusive).",
        ),
        "end_date": Param(
            "", type=["string", "null"],
            description="Backfill end date, YYYY-MM-DD (inclusive). Leave blank for today.",
        ),
    },
) as dag:

    resolve_scope = PythonOperator(
        task_id="resolve_backfill_scope",
        python_callable=_resolve_backfill_scope,
    )

    extract_backfill = spark_job_operator(
        task_id="extract_backfill",
        job_script="bronze_ingestion.py",
        arguments=[
            "--dataset", "{{ ti.xcom_pull(task_ids='resolve_backfill_scope', key='dataset_arg') }}",
            "--start", "{{ ti.xcom_pull(task_ids='resolve_backfill_scope', key='start_date') }}",
            "--end", "{{ ti.xcom_pull(task_ids='resolve_backfill_scope', key='end_date') }}",
        ],
    )

    transform_backfill = spark_job_operator(
        task_id="transform_backfill",
        job_script="silver_transform.py",
        arguments=["--dataset", "{{ ti.xcom_pull(task_ids='resolve_backfill_scope', key='dataset_arg') }}"],
    )

    resolve_scope >> extract_backfill >> transform_backfill
