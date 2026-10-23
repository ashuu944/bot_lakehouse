"""Builds catalog.json: a snapshot of every Bronze/Silver/Gold table -- schema,
row/institution counts, date coverage, and curated description/suggested-
analysis text for each.

NOTE: the catalog-portal nginx container that used to serve this as a static
page (catalog/index.html) has been removed -- OpenMetadata is now the single
catalog UI. This job (and the BSIS_DESCRIPTIONS/MASTER_DESCRIPTIONS/
GOLD_DESCRIPTIONS/suggested-analysis text below) is kept as the source data
for pushing that same curated info into OpenMetadata (descriptions, and a
custom property for suggested-analysis/row counts/institution counts/date
range, since OpenMetadata's own profiler can't currently produce those for
Bronze/Silver -- see openmetadata_ingestion_dag.py's profile_gold docstring
for the upstream bug) -- not for catalog/index.html, which is unused now.

Reuses the dataset lists already defined elsewhere (bsis_frequency,
silver_master_transform, silver_transform, gold_transform) instead of
redeclaring them, so the catalog can't silently drift out of sync with the
datasets those jobs actually produce.

Usage:
    docker compose exec spark spark-submit /opt/spark-jobs/catalog_builder.py
"""

import json
import logging
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from bsis_frequency import frequency_of
from catalog_descriptions import BSIS_DESCRIPTIONS, GOLD_DESCRIPTIONS, MASTER_DESCRIPTIONS
from gold_transform import BUILDERS as GOLD_TABLES
from silver_master_transform import MASTER_DATASETS
from source_config import build_dataset_index
from spark_session import get_spark, lakehouse_path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("catalog_builder")

OUT_PATH = "/opt/catalog/catalog.json"

# DATASET_DOMAIN/BSIS_DATASETS used to be BSIS-specific dicts hardcoded in
# silver_transform.py; now that every dataset's domain/directorate/source
# system lives in spark/config/sources/**/*.yaml, derive the same lookups
# from there instead of re-declaring them here.
_DATASET_INDEX = build_dataset_index()
BSIS_DATASETS = sorted(
    name for name, entry in _DATASET_INDEX.items()
    if entry["directorate"] == "DFSS" and entry["source_system"] == "BSIS"
)
DATASET_DOMAIN = {name: _DATASET_INDEX[name]["domain"] for name in BSIS_DATASETS}


def _find_column(columns: list[str], candidates: list[str]) -> str | None:
    lower = {c.lower(): c for c in columns}
    for candidate in candidates:
        if candidate in lower:
            return lower[candidate]
    return None


def inspect_table(df: DataFrame) -> dict:
    columns = df.columns
    row_count = df.count()

    date_col = _find_column(columns, ["report_date", "reporting_date", "dateval"])
    date_range = None
    if date_col:
        row = df.select(F.min(date_col).alias("lo"), F.max(date_col).alias("hi")).first()
        if row and row["lo"] is not None:
            date_range = {"min": str(row["lo"]), "max": str(row["hi"])}

    inst_col = _find_column(columns, ["institution_code", "institutioncode"])
    institution_count = df.select(inst_col).distinct().count() if inst_col else None

    return {
        "row_count": row_count,
        "columns": [{"name": f.name, "type": f.dataType.simpleString()} for f in df.schema.fields],
        "date_range": date_range,
        "institution_count": institution_count,
    }


def bronze_entry(
    spark: SparkSession, source: str, dataset: str, description: str, suggested_analysis: str | None,
    path_parts: tuple[str, ...] | None = None,
) -> dict | None:
    # path_parts is the real physical location (e.g. ("DFSS", "BSIS")); source
    # stays the catalog.json grouping key this module's own layers dict is
    # organized by -- the directorate/source split lives in storage, not in
    # that grouping.
    # Both BSIS and master Bronze are Delta now (one logical table per
    # dataset, schema from the Delta log) -- master used to be plain Parquet,
    # moved to Delta so it could be registered into Trino/OpenMetadata the
    # same way BSIS Bronze already was (see trino/register_bronze_tables.sh).
    path = lakehouse_path("bronze", *(path_parts or (source,)), dataset)
    try:
        df = spark.read.format("delta").load(path)
    except Exception:
        logger.warning("Skipping bronze/%s/%s -- no data yet (%s)", source, dataset, path)
        return None

    info = inspect_table(df)
    last_updated = None
    if "extraction_ts" in df.columns:
        row = df.select(F.max("extraction_ts").alias("ts")).first()
        last_updated = str(row["ts"]) if row and row["ts"] is not None else None

    return {
        "name": dataset,
        "description": description,
        "suggested_analysis": suggested_analysis,
        "cadence": frequency_of(dataset) if source == "bsis" else "reference (overwritten each run)",
        "format": fmt,
        "path": path,
        "last_updated": last_updated,
        **info,
    }


def delta_entry(
    spark: SparkSession, layer: str, source: str, dataset: str, description: str, suggested_analysis: str | None,
    path_parts: tuple[str, ...] | None = None,
) -> dict | None:
    path = lakehouse_path(layer, *(path_parts or (source,)), dataset)
    try:
        df = spark.read.format("delta").load(path)
    except Exception:
        logger.warning("Skipping %s/%s/%s -- no data yet (%s)", layer, source, dataset, path)
        return None

    info = inspect_table(df)
    last_updated = None
    try:
        history = DeltaTable.forPath(spark, path).history(1).select("timestamp").first()
        last_updated = str(history["timestamp"]) if history else None
    except Exception:
        pass

    return {
        "name": dataset,
        "description": description,
        "suggested_analysis": suggested_analysis,
        "cadence": frequency_of(dataset) if source == "bsis" and layer == "silver" else (
            "reference (overwritten each run)" if layer == "silver" else None
        ),
        "format": "delta",
        "path": path,
        "last_updated": last_updated,
        **info,
    }


def main():
    spark = get_spark("catalog-builder")
    catalog = {"generated_at": datetime.now(timezone.utc).isoformat(), "layers": {}}

    logger.info("Inspecting bronze/DFSS/BSIS (%d datasets)", len(BSIS_DATASETS))
    bronze_bsis = [
        bronze_entry(spark, "bsis", ds, *BSIS_DESCRIPTIONS[ds], path_parts=("DFSS", "BSIS")) for ds in BSIS_DATASETS
    ]
    logger.info("Inspecting bronze/DFSS/BSIS/master (%d datasets)", len(MASTER_DATASETS))
    bronze_master = [
        bronze_entry(
            spark, "master", ds, MASTER_DESCRIPTIONS.get(ds, ""), None,
            path_parts=("DFSS", "BSIS", "master"),
        )
        for ds in MASTER_DATASETS
    ]

    logger.info("Inspecting silver/DFSS/<domain> (%d datasets)", len(BSIS_DATASETS))
    silver_bsis = [
        delta_entry(spark, "silver", "bsis", ds, *BSIS_DESCRIPTIONS[ds], path_parts=("DFSS", DATASET_DOMAIN[ds]))
        for ds in BSIS_DATASETS
    ]
    logger.info("Inspecting silver/DFSS/BSIS/master (%d datasets)", len(MASTER_DATASETS))
    silver_master = [
        delta_entry(
            spark, "silver", "master", ds, MASTER_DESCRIPTIONS.get(ds, ""), None,
            path_parts=("DFSS", "BSIS", "master"),
        )
        for ds in MASTER_DATASETS
    ]

    logger.info("Inspecting gold/bsis (%d tables)", len(GOLD_TABLES))
    gold_bsis = [
        delta_entry(spark, "gold", "bsis", ds, *GOLD_DESCRIPTIONS[ds]) for ds in sorted(GOLD_TABLES)
    ]

    catalog["layers"]["bronze"] = {
        "bsis": [e for e in bronze_bsis if e],
        "master": [e for e in bronze_master if e],
    }
    catalog["layers"]["silver"] = {
        "bsis": [e for e in silver_bsis if e],
        "master": [e for e in silver_master if e],
    }
    catalog["layers"]["gold"] = {
        "bsis": [e for e in gold_bsis if e],
    }

    with open(OUT_PATH, "w") as f:
        json.dump(catalog, f, indent=2, default=str)
    logger.info("Wrote catalog to %s", OUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
