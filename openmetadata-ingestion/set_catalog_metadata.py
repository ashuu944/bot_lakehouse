"""Pushes curated table descriptions, suggested-analysis text, cadence, and
Trino-computed row/institution counts + date coverage into OpenMetadata --
the OpenMetadata-side replacement for what the now-removed catalog-portal
(catalog/index.html) used to show. See spark/jobs/catalog_builder.py's
docstring for the fuller rationale.

Why custom properties instead of OpenMetadata's own Profiler: this
deployment's profiler workflow is broken upstream for openmetadata-
ingestion==2.0.5 (see openmetadata_ingestion_dag.py's profile_gold
docstring), and Bronze/Silver aren't profiled at all today even when it
works. Row/institution counts and date coverage here are computed with a
direct Trino query instead -- this script runs in the lightweight
openmetadata-ingestion container, which has no pyspark, so it can't reuse
catalog_builder.py's Spark-based inspect_table() -- but Bronze/Silver/Gold
are all registered into Trino already (same tables the metadata-ingest
workflows themselves read), so querying Trino directly needs nothing extra.

Run this AFTER the Bronze/Silver/Gold metadata-ingest workflows (it patches
tables that must already exist in OpenMetadata) -- see
airflow/dags/openmetadata_ingestion_dag.py.

Covers master/reference datasets (institutions, sectors, etc.) too, now that
trino/register_bronze_tables.sh and register_silver_tables.sh register them
into the same delta.bronze/delta.silver schemas as the 12 BSIS fact
datasets -- the existing trino_bronze_metadata.yaml/trino_silver_metadata.yaml
connectors (pointed at those whole schemas) already pick them up as Table
entities with no config change, this script just needs to know their names
too (MASTER_DESCRIPTIONS, below).

Usage:
    python set_catalog_metadata.py
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone

_SPARK_JOBS_DIR = "/opt/spark-jobs"
if _SPARK_JOBS_DIR not in sys.path:
    sys.path.insert(0, _SPARK_JOBS_DIR)

import trino  # noqa: E402

from bsis_frequency import frequency_of  # noqa: E402
from catalog_descriptions import BSIS_DESCRIPTIONS, GOLD_DESCRIPTIONS, MASTER_DESCRIPTIONS  # noqa: E402
from source_config import build_dataset_index  # noqa: E402

from metadata.generated.schema.api.data.createCustomProperty import CreateCustomPropertyRequest  # noqa: E402
from metadata.generated.schema.entity.data.table import Table  # noqa: E402
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (  # noqa: E402
    AuthProvider,
    OpenMetadataConnection,
)
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (  # noqa: E402
    OpenMetadataJWTClientConfig,
)
from metadata.ingestion.models.custom_properties import CustomPropertyDataTypes, OMetaCustomProperties  # noqa: E402
from metadata.ingestion.ometa.ometa_api import OpenMetadata  # noqa: E402

#  force=True: same reasoning as set_governance.py's logging.basicConfig call
# -- the openmetadata-ingestion package configures the root logger as a side
# effect of the imports above, so a plain basicConfig() here would be a no-op.
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", force=True)
logger = logging.getLogger("set_catalog_metadata")

TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
TRINO_USERNAME = os.environ.get("TRINO_USERNAME", "lakehouse_ingestion")

# Custom property name -> (OpenMetadata data type, description shown in the
# OpenMetadata UI's "Add custom property" / table-schema admin screen).
CUSTOM_PROPERTIES: dict[str, tuple[CustomPropertyDataTypes, str]] = {
    "suggestedAnalysis": (
        CustomPropertyDataTypes.MARKDOWN,
        "What this dataset is good for analytically -- curated text from spark/jobs/catalog_descriptions.py.",
    ),
    "cadence": (
        CustomPropertyDataTypes.STRING,
        "How often this dataset is refreshed: daily, monthly, or reference (overwritten each run).",
    ),
    "rowCount": (
        CustomPropertyDataTypes.INTEGER,
        "Row count, computed via a direct Trino query at the last catalog-metadata run.",
    ),
    "institutionCount": (
        CustomPropertyDataTypes.INTEGER,
        "Distinct institution_code count, computed via Trino (only set when the table has that column).",
    ),
    "dateRangeMin": (
        CustomPropertyDataTypes.STRING,
        "Earliest report_date-like value, computed via Trino (only set when the table has such a column).",
    ),
    "dateRangeMax": (
        CustomPropertyDataTypes.STRING,
        "Latest report_date-like value, computed via Trino (only set when the table has such a column).",
    ),
    "statsUpdatedAt": (
        CustomPropertyDataTypes.STRING,
        "UTC timestamp of the last time rowCount/institutionCount/dateRange were computed.",
    ),
}

_DATE_COLUMN_CANDIDATES = ("report_date", "reporting_date", "dateval")
_INSTITUTION_COLUMN_CANDIDATES = ("institution_code", "institutioncode")

# Matches catalog_builder.py's own cadence label for master data -- no fixed
# reporting frequency, each run just overwrites with the reference file's
# current state.
MASTER_CADENCE = "reference (overwritten each run)"


def get_client() -> OpenMetadata:
    server_config = OpenMetadataConnection(
        hostPort=os.environ.get("OM_SERVER_HOST_PORT", "http://om-server:8585/api"),
        authProvider=AuthProvider.openmetadata,
        securityConfig=OpenMetadataJWTClientConfig(jwtToken=os.environ["OM_INGESTION_BOT_TOKEN"]),
    )
    return OpenMetadata(server_config)


def ensure_custom_properties(metadata: OpenMetadata) -> None:
    """Registers each name in CUSTOM_PROPERTIES on the Table entity type, if
    not already there. One-time schema setup; safe to re-run (skips names
    that already exist) -- see set_governance.py's ensure_team/ensure_domain
    for the same "idempotent ensure" pattern applied to a different entity.
    """
    existing = {prop["name"] for prop in (metadata.get_entity_custom_properties(Table) or [])}
    for name, (data_type, description) in CUSTOM_PROPERTIES.items():
        if name in existing:
            continue
        logger.info("Registering custom property %r (%s) on Table", name, data_type.value)
        metadata.create_or_update_custom_property(
            OMetaCustomProperties(
                entity_type=Table,
                createCustomPropertyRequest=CreateCustomPropertyRequest(
                    name=name,
                    description=description,
                    propertyType=metadata.get_property_type_ref(data_type),
                ),
            )
        )


def _find_column(columns: list[str], candidates: tuple[str, ...]) -> str | None:
    lower = {c.lower(): c for c in columns}
    for candidate in candidates:
        if candidate in lower:
            return lower[candidate]
    return None


def compute_stats(cur, schema: str, table: str) -> dict:
    """rowCount always; institutionCount/dateRangeMin/Max only when the table
    has a matching column -- same column-sniffing catalog_builder.py's
    inspect_table() does, just via Trino SQL instead of a Spark DataFrame.
    """
    cur.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ?",
        (schema, table),
    )
    columns = [row[0] for row in cur.fetchall()]
    if not columns:
        logger.warning("No such Trino table delta.%s.%s -- skipping stats", schema, table)
        return {}

    stats: dict = {}
    cur.execute(f'SELECT count(*) FROM delta."{schema}"."{table}"')
    stats["rowCount"] = cur.fetchone()[0]

    date_col = _find_column(columns, _DATE_COLUMN_CANDIDATES)
    if date_col:
        cur.execute(f'SELECT min("{date_col}"), max("{date_col}") FROM delta."{schema}"."{table}"')
        lo, hi = cur.fetchone()
        if lo is not None:
            stats["dateRangeMin"] = str(lo)
            stats["dateRangeMax"] = str(hi)

    inst_col = _find_column(columns, _INSTITUTION_COLUMN_CANDIDATES)
    if inst_col:
        cur.execute(f'SELECT count(DISTINCT "{inst_col}") FROM delta."{schema}"."{table}"')
        stats["institutionCount"] = cur.fetchone()[0]

    return stats


def apply_catalog_metadata(
    metadata: OpenMetadata,
    cur,
    fqn: str,
    schema: str,
    trino_table: str,
    description: str,
    suggested_analysis: str | None,
    cadence: str | None,
) -> None:
    table = metadata.get_by_name(entity=Table, fqn=fqn)
    if table is None:
        logger.warning("Table not found (not ingested yet?): %s", fqn)
        return

    metadata.patch_description(entity=Table, source=table, description=description, force=True)

    custom_properties = {
        key: value
        for key, value in {"suggestedAnalysis": suggested_analysis, "cadence": cadence}.items()
        if value is not None
    }
    custom_properties.update(compute_stats(cur, schema, trino_table))
    if custom_properties:
        custom_properties["statsUpdatedAt"] = datetime.now(timezone.utc).isoformat()
        metadata.patch_custom_properties(entity=Table, entity_id=table.id, custom_properties=custom_properties)

    logger.info("Set catalog metadata on %s", fqn)


def main():
    metadata = get_client()
    ensure_custom_properties(metadata)

    dataset_index = build_dataset_index()
    bsis_datasets = sorted(
        name
        for name, entry in dataset_index.items()
        if entry["directorate"] == "DFSS" and entry["source_system"] == "BSIS"
    )

    conn = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USERNAME, catalog="delta")
    cur = conn.cursor()

    for dataset in bsis_datasets:
        description, suggested_analysis = BSIS_DESCRIPTIONS[dataset]
        cadence = frequency_of(dataset)
        apply_catalog_metadata(
            metadata, cur,
            fqn=f"lakehouse-trino-bronze.delta.bronze.{dataset}",
            schema="bronze", trino_table=dataset,
            description=description, suggested_analysis=suggested_analysis, cadence=cadence,
        )
        apply_catalog_metadata(
            metadata, cur,
            fqn=f"lakehouse-trino-silver.delta.silver.{dataset}",
            schema="silver", trino_table=dataset,
            description=description, suggested_analysis=suggested_analysis, cadence=cadence,
        )

    for dataset, description in MASTER_DESCRIPTIONS.items():
        apply_catalog_metadata(
            metadata, cur,
            fqn=f"lakehouse-trino-bronze.delta.bronze.{dataset}",
            schema="bronze", trino_table=dataset,
            description=description, suggested_analysis=None, cadence=MASTER_CADENCE,
        )
        apply_catalog_metadata(
            metadata, cur,
            fqn=f"lakehouse-trino-silver.delta.silver.{dataset}",
            schema="silver", trino_table=dataset,
            description=description, suggested_analysis=None, cadence=MASTER_CADENCE,
        )

    for table_name, (description, suggested_analysis) in GOLD_DESCRIPTIONS.items():
        apply_catalog_metadata(
            metadata, cur,
            fqn=f"lakehouse-trino-gold.delta.gold.{table_name}",
            schema="gold", trino_table=table_name,
            description=description, suggested_analysis=suggested_analysis, cadence=None,
        )

    cur.close()
    conn.close()
    logger.info("Catalog metadata applied.")


if __name__ == "__main__":
    main()
