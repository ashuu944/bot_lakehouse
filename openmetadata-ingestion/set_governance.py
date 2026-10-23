"""Sets owner (directorate, as an OpenMetadata Team) and domain (business
domain, as an OpenMetadata Domain) on every Bronze/Silver table this
lakehouse has ingested -- driven by the same spark/config/sources/**/*.yaml
bronze_ingestion.py/silver_transform.py use, so a dataset's owner/domain is
declared in exactly one place, not re-typed into OpenMetadata separately.

Run this AFTER the Bronze/Silver/Gold metadata-ingest workflows (it patches
tables that must already exist in OpenMetadata) -- see
airflow/dags/openmetadata_ingestion_dag.py.

Gold is handled separately (GOLD_GOVERNANCE below): Gold tables are
cross-domain marts by design (e.g. institution_risk_trends combines
liquidity_risk + capital_adequacy ratios side by side), so their domain
isn't derivable from a single dataset's Silver config the way Bronze/Silver's
is -- it's a judgment call, made explicitly here instead of guessed.

Usage:
    python set_governance.py
"""

from __future__ import annotations

import logging
import os
import sys

_SPARK_JOBS_DIR = "/opt/spark-jobs"
if _SPARK_JOBS_DIR not in sys.path:
    sys.path.insert(0, _SPARK_JOBS_DIR)

from source_config import build_dataset_index  # noqa: E402

from metadata.generated.schema.api.domains.createDomain import CreateDomainRequest  # noqa: E402
from metadata.generated.schema.api.teams.createTeam import CreateTeamRequest  # noqa: E402
from metadata.generated.schema.entity.data.table import Table  # noqa: E402
from metadata.generated.schema.entity.domains.domain import DomainType  # noqa: E402
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (  # noqa: E402
    AuthProvider,
    OpenMetadataConnection,
)
from metadata.generated.schema.entity.teams.team import TeamType  # noqa: E402
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (  # noqa: E402
    OpenMetadataJWTClientConfig,
)
from metadata.generated.schema.type.entityReference import EntityReference  # noqa: E402
from metadata.generated.schema.type.entityReferenceList import EntityReferenceList  # noqa: E402
from metadata.ingestion.ometa.ometa_api import OpenMetadata  # noqa: E402

#  force=True: the openmetadata-ingestion package configures the root logger
# as a side effect of the imports above, so a plain basicConfig() here would
# be a no-op (Python only applies it if no handlers exist yet) and silently
# swallow this script's own INFO logs.
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", force=True)
logger = logging.getLogger("set_governance")

# One domain per Gold table, even though institution_risk_trends genuinely
# draws from two (liquidity_risk + capital_adequacy ratios side by side):
# this OpenMetadata instance has the default "Multiple Domains are not
# allowed" governance rule active (confirmed by the server rejecting a
# 2-domain patch), so a cross-domain mart has to pick one primary domain
# rather than represent both.
GOLD_GOVERNANCE = {
    "sector_exposure": {"directorate": "DFSS", "domain": "credit_risk"},
    "institution_risk_trends": {"directorate": "DFSS", "domain": "liquidity_risk"},
}


def get_client() -> OpenMetadata:
    server_config = OpenMetadataConnection(
        hostPort=os.environ.get("OM_SERVER_HOST_PORT", "http://om-server:8585/api"),
        authProvider=AuthProvider.openmetadata,
        securityConfig=OpenMetadataJWTClientConfig(jwtToken=os.environ["OM_INGESTION_BOT_TOKEN"]),
    )
    return OpenMetadata(server_config)


def ensure_team(metadata: OpenMetadata, name: str) -> EntityReference:
    # teamType must be Group -- OpenMetadata only allows a Group-type team to
    # own entities (Department/Division/BusinessUnit are org-structure-only
    # and were rejected outright: "Team of type Department can't own
    # entities"). A directorate maps cleanly to a Group here since it's
    # exactly the thing that owns this lakehouse's data; a fuller org
    # hierarchy (directorate as Department containing per-source-system
    # Groups) is a later refinement if ownership ever needs to be finer than
    # "the whole directorate", not a need today.
    team = metadata.create_or_update(
        CreateTeamRequest(
            name=name,
            displayName=name,
            teamType=TeamType.Group,
            description=f"Owns data sourced under the {name} directorate.",
        )
    )
    return EntityReference(id=team.id, type="team", name=str(team.name))


def ensure_domain(metadata: OpenMetadata, name: str) -> EntityReference:
    domain = metadata.create_or_update(
        CreateDomainRequest(
            name=name,
            displayName=name.replace("_", " ").title(),
            # Aggregate: these risk domains are organized by business meaning,
            # not tied to one source system -- a future second source feeding
            # the same domain (e.g. another directorate's credit data) would
            # join the same Domain, not get its own.
            domainType=DomainType.Aggregate,
            description=f"Assets relevant to {name.replace('_', ' ')} analysis, regardless of source system.",
        )
    )
    return EntityReference(id=domain.id, type="domain", name=str(domain.name))


def apply_governance(metadata: OpenMetadata, fqn: str, owner_ref: EntityReference, domain_refs: list[EntityReference]) -> None:
    table = metadata.get_by_name(entity=Table, fqn=fqn)
    if table is None:
        logger.warning("Table not found (not ingested yet?): %s", fqn)
        return
    metadata.patch_owner(entity=Table, source=table, owners=EntityReferenceList(root=[owner_ref]), force=True)
    metadata.patch_domain(entity=Table, source=table, domains=EntityReferenceList(root=domain_refs))
    logger.info("Governed %s -> owner=%s domains=%s", fqn, owner_ref.name, [d.name for d in domain_refs])


def main():
    metadata = get_client()
    dataset_index = build_dataset_index()

    team_cache: dict[str, EntityReference] = {}
    domain_cache: dict[str, EntityReference] = {}

    def team_ref(directorate: str) -> EntityReference:
        if directorate not in team_cache:
            team_cache[directorate] = ensure_team(metadata, directorate)
        return team_cache[directorate]

    def domain_ref(domain: str) -> EntityReference:
        if domain not in domain_cache:
            domain_cache[domain] = ensure_domain(metadata, domain)
        return domain_cache[domain]

    for dataset, entry in dataset_index.items():
        owner = team_ref(entry["directorate"])
        domain = domain_ref(entry["domain"])

        apply_governance(metadata, f"lakehouse-trino-bronze.delta.bronze.{dataset}", owner, [domain])
        apply_governance(metadata, f"lakehouse-trino-silver.delta.silver.{dataset}", owner, [domain])

    for table, gov in GOLD_GOVERNANCE.items():
        owner = team_ref(gov["directorate"])
        domain = domain_ref(gov["domain"])
        apply_governance(metadata, f"lakehouse-trino-gold.delta.gold.{table}", owner, [domain])

    logger.info("Governance applied.")


if __name__ == "__main__":
    main()
