"""OpenMetadata ingestion: schema metadata, ownership/domain governance,
Bronze->Silver->Gold lineage, curated descriptions/suggested-analysis/
cadence/row-institution-counts/date-coverage, and data quality profiling for
the whole lakehouse, via Trino -- scheduled from our own Airflow instead of
OpenMetadata's bundled ingestion-Airflow (see om_ingestion_helpers.py).

Lineage is drawn by set_lineage.py, NOT OpenLineage/Spark-listener-based
automatic lineage: that was investigated and ruled out because
openmetadata-ingestion==2.0.5's OpenLineage pipeline source only resolves
dataset identities shaped like SQL-warehouse names, AWS Glue ARNs, or Azure
Kusto/Cosmos URIs -- it has no handler for a raw s3://bucket/key path, so
Spark reading/writing Delta tables directly on MinIO would never resolve
back to our Trino-registered tables, even with a Kafka broker and the
openlineage-spark listener in place. set_lineage.py's Bronze->Silver edges
are fully automatic (driven by the same source_config.py dataset index
set_governance.py uses); Silver->Gold is a hand-maintained declaration,
same reasoning as GOLD_GOVERNANCE in set_governance.py -- see set_lineage.py's
own docstring.

Bronze/Silver/Gold are ALL registered into Trino first (trino/register_
bronze_tables.sh, register_silver_tables.sh, register_gold_tables.py) and
ingested from there via the Trino connector -- one uniform mechanism across
every layer. Bronze specifically used to go through OpenMetadata's S3
Datalake connector straight against MinIO; that connector has no concept of
a Hive-partitioned directory being ONE table, so it listed every
reporting_date partition FILE as its own "table" (355 of them for 12 real
datasets). Bronze is Delta now (bronze_ingestion.py), so it's visible the
same safe way Silver/Gold always have been.

Ingestion order matters: ingest_*_schema must run before enrich_governance,
enrich_lineage, and enrich_catalog_metadata (all three patch tables that must
already exist), which must run before profile_gold (profiling needs the
tables to exist too, though it doesn't depend on
governance/lineage/catalog-metadata specifically -- ordered after them here
just to keep the DAG a single chain rather than a wider fan-in/out for no
real benefit).

Requires OM_INGESTION_BOT_TOKEN in .env (see om_ingestion_helpers.py for how
to get it -- it can't be generated ahead of the server's first login).

KNOWN ISSUE (profile_gold): openmetadata-ingestion==2.0.5's Profiler workflow
raises `'DatabaseServiceProfilerPipeline' object has no attribute 'threads'`
on ANY config, including the documented minimal example verbatim -- confirmed
by running `docker run ... metadata ingest -c trino_gold_profiler.yaml`
directly outside Airflow, stripped down to the bare docs example, same
AttributeError every time. Not caused by anything in trino_gold_profiler.yaml.
No matching GitHub issue found as of 2026-10-20. profile_gold is left wired
up (so it starts working automatically once this is fixed upstream, no DAG
change needed) but will fail until then. Workaround meanwhile: run
profiling/add Data Quality test cases from the OpenMetadata UI's Profiler tab
directly on gold.sector_exposure / gold.institution_risk_trends, a different
code path than this CLI workflow.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG

from om_ingestion_helpers import (
    om_catalog_metadata_operator,
    om_governance_operator,
    om_ingest_operator,
    om_lineage_operator,
)

TZ = "Africa/Dar_es_Salaam"

default_args = {
    "owner": "lakehouse-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="openmetadata_lakehouse_ingestion",
    description="OpenMetadata: ingest Bronze/Silver/Gold schema from Trino, apply owner/domain governance, then profile Gold",
    # After the daily/monthly Silver transforms would plausibly have run and
    # Gold been rebuilt -- revisit once gold_transform.py itself is on a
    # schedule; for now this just needs to run sometime after the lakehouse
    # changes.
    schedule="0 8 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz=TZ),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["openmetadata", "bronze", "silver", "gold", "lineage", "governance", "quality"],
) as dag:

    ingest_bronze = om_ingest_operator(
        task_id="ingest_bronze_schema",
        config_file="trino_bronze_metadata.yaml",
    )

    ingest_silver = om_ingest_operator(
        task_id="ingest_silver_schema",
        config_file="trino_silver_metadata.yaml",
    )

    ingest_gold = om_ingest_operator(
        task_id="ingest_gold_schema",
        config_file="trino_gold_metadata.yaml",
    )

    enrich_governance = om_governance_operator()
    enrich_lineage = om_lineage_operator()
    enrich_catalog_metadata = om_catalog_metadata_operator()

    profile_gold = om_ingest_operator(
        task_id="profile_gold",
        config_file="trino_gold_profiler.yaml",
    )

    (
        [ingest_bronze, ingest_silver, ingest_gold]
        >> enrich_governance
        >> enrich_lineage
        >> enrich_catalog_metadata
        >> profile_gold
    )
