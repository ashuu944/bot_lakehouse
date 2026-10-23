"""Draws Bronze->Silver->Gold lineage edges in OpenMetadata.

Bronze->Silver is driven entirely by spark/config/sources/**/*.yaml (the same
source_config.py set_governance.py uses): every dataset there has exactly one
Bronze table and one Silver table sharing its name, so a NEW dataset added to
that config automatically gets its lineage edge too, with no change needed
here.

Silver->Gold is NOT derivable from that config -- Gold tables are bespoke
marts (gold_transform.py's own docstring: "adding a new Gold table should
always be traceable to a named use case, not built speculatively"), each
reading whichever specific Silver tables its own Python builder function
chooses to. That choice lives in gold_transform.py's code, not in a config a
script can introspect, so GOLD_LINEAGE below is a deliberate, hand-maintained
declaration -- update it whenever gold_transform.py's builders change which
Silver tables they read. (Investigated and ruled out: OpenLineage/Spark-
listener-based automatic lineage, which WOULD cover this case automatically
-- openmetadata-ingestion==2.0.5's OpenLineage pipeline source only resolves
dataset identities shaped like SQL-warehouse names, AWS Glue ARNs, or Azure
Kusto/Cosmos URIs; it has no handler for a raw s3://bucket/key path, so Spark
reading/writing Delta tables directly on MinIO would never resolve back to
our Trino-registered tables. See conversation notes, 2026-10-21.)

Run this AFTER the Bronze/Silver/Gold metadata-ingest workflows (it links
tables that must already exist in OpenMetadata) -- see
airflow/dags/openmetadata_ingestion_dag.py.

Usage:
    python set_lineage.py
"""

from __future__ import annotations

import logging
import os
import sys

_SPARK_JOBS_DIR = "/opt/spark-jobs"
if _SPARK_JOBS_DIR not in sys.path:
    sys.path.insert(0, _SPARK_JOBS_DIR)

from source_config import build_dataset_index  # noqa: E402

from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (  # noqa: E402
    AuthProvider,
    OpenMetadataConnection,
)
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (  # noqa: E402
    OpenMetadataJWTClientConfig,
)
from metadata.ingestion.ometa.ometa_api import OpenMetadata  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", force=True)
logger = logging.getLogger("set_lineage")

# Gold table -> the Silver tables its builder function actually reads
# (gold_transform.py). Hand-maintained -- see module docstring for why.
GOLD_LINEAGE = {
    "sector_exposure": ["credit_exposure"],
    "institution_risk_trends": ["lcr", "nsfr", "capital_adequacy"],
}


def get_client() -> OpenMetadata:
    server_config = OpenMetadataConnection(
        hostPort=os.environ.get("OM_SERVER_HOST_PORT", "http://om-server:8585/api"),
        authProvider=AuthProvider.openmetadata,
        securityConfig=OpenMetadataJWTClientConfig(jwtToken=os.environ["OM_INGESTION_BOT_TOKEN"]),
    )
    return OpenMetadata(server_config)


def link(metadata: OpenMetadata, from_fqn: str, to_fqn: str) -> None:
    result = metadata.add_lineage_by_name(
        from_entity_fqn=from_fqn,
        from_entity_type="table",
        to_entity_fqn=to_fqn,
        to_entity_type="table",
        return_lineage=False,
    )
    if "error" in result:
        logger.warning("Failed to link %s -> %s: %s", from_fqn, to_fqn, result["error"])
    else:
        logger.info("Linked %s -> %s", from_fqn, to_fqn)


def main():
    metadata = get_client()
    dataset_index = build_dataset_index()

    for dataset in dataset_index:
        bronze_fqn = f"lakehouse-trino-bronze.delta.bronze.{dataset}"
        silver_fqn = f"lakehouse-trino-silver.delta.silver.{dataset}"
        link(metadata, bronze_fqn, silver_fqn)

    for gold_table, silver_tables in GOLD_LINEAGE.items():
        gold_fqn = f"lakehouse-trino-gold.delta.gold.{gold_table}"
        for silver_table in silver_tables:
            silver_fqn = f"lakehouse-trino-silver.delta.silver.{silver_table}"
            link(metadata, silver_fqn, gold_fqn)

    logger.info("Lineage applied.")


if __name__ == "__main__":
    main()
