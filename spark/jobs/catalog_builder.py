"""Builds catalog.json: a snapshot of every Bronze/Silver/Gold table for the
data portal (catalog/index.html, served by the catalog-portal nginx
container) -- lets a human see what data exists, its schema, how much
history it covers, and what it's actually for, without needing MinIO
credentials or a Parquet/Delta reader.

Reuses the dataset lists already defined elsewhere (bsis_frequency,
silver_master_transform, silver_transform, gold_transform) instead of
redeclaring them, so the catalog can't silently drift out of sync with the
datasets those jobs actually produce.

Written to a local path (not MinIO) because this is metadata ABOUT the
lakehouse, not lakehouse data itself, and the static file server serving the
portal needs it on a plain filesystem path.

Usage:
    docker compose exec spark spark-submit /opt/spark-jobs/catalog_builder.py
    (then refresh http://localhost:8090 -- re-run any time after a Bronze/
    Silver/Gold job, no restart needed)
"""

import json
import logging
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from bsis_frequency import frequency_of
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

BSIS_DESCRIPTIONS = {
    "smr_daily_compliance": ("Daily statutory minimum reserve (SMR) compliance per institution.",
                              "Day-to-day SMR breach detection, compliance trend."),
    "liquidity_compliance": ("Daily liquid-assets-to-demand-liabilities compliance ratio.",
                              "Daily liquidity compliance trend, breach alerts."),
    "net_open_position": ("Daily net open foreign-exchange position.",
                           "FX risk exposure monitoring."),
    "lcr": ("Monthly Liquidity Coverage Ratio (Basel III).",
             "Liquidity risk trend analysis, regulatory threshold breaches."),
    "nsfr": ("Monthly Net Stable Funding Ratio (Basel III).",
              "Funding stability trend analysis."),
    "capital_adequacy": ("Monthly capital adequacy ratios (Tier 1, Total Capital, Leverage).",
                          "Solvency/capital-strength trend analysis -- CAMELS 'C'."),
    "sensitivity_market_risk": ("Monthly market risk sensitivity measures.",
                                 "Interest-rate / market-risk exposure analysis."),
    "asset_quality": ("Monthly asset quality rating and related metrics.",
                       "Asset quality trend analysis -- CAMELS 'A'."),
    "credit_exposure": ("Monthly credit exposure by economic sector, with performing/non-performing breakdown.",
                         "Sector concentration risk, NPL-by-sector, institution-sector exposure graphs."),
    "balance_sheet": ("Monthly full balance sheet, one column per line item (pivoted from Oracle's long format).",
                       "Balance sheet structure analysis, peer comparison."),
    "earnings": ("Monthly earnings/profitability metrics.",
                 "Profitability trend analysis -- CAMELS 'E'."),
    "income_statement": ("Monthly income statement, monthly + year-to-date amount per line item (pivoted).",
                          "P&L trend analysis, revenue/expense breakdown."),
}

MASTER_DESCRIPTIONS = {
    "institutions": "Registered financial institutions (code, name, status) -- the join key for every BSIS fact table.",
    "institution_categories": "Institution category reference (e.g. commercial bank, community bank).",
    "institution_types": "Institution type reference.",
    "sectors": "Economic sector reference -- joined against credit_exposure.",
    "risk_types": "Risk type reference used across risk datasets.",
    "regulatory_thresholds": "Regulatory minimum/maximum thresholds per ratio (e.g. minimum LCR).",
    "reporting_frequencies": "Reference for how often each BSIS report is submitted.",
    "reporting_obligations": "Which institutions must submit which reports.",
    "dataset_types": "BSIS dataset/report type reference.",
    "camels_rating_criteria": "CAMELS rating scale/criteria reference.",
    "holidays": "Bank holiday calendar -- relevant for daily-report date logic.",
}

GOLD_DESCRIPTIONS = {
    "sector_exposure": (
        "Institution x sector x month credit exposure, with total_exposure and npl_ratio derived.",
        "Sector concentration risk, NPL-by-sector; edge-shaped for a future Neo4j institution-sector graph.",
    ),
    "institution_risk_trends": (
        "Institution x month view of LCR, NSFR, and capital-adequacy ratios side by side.",
        "Power BI trend dashboards: one ratio's trend per institution, or compare institutions.",
    ),
}


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
    path_parts: tuple[str, ...] | None = None, fmt: str = "delta",
) -> dict | None:
    # path_parts is the real physical location (e.g. ("DFSS", "BSIS")); source
    # stays the catalog.json grouping key the portal already renders -- the
    # directorate/source split lives in storage, not in the portal's JSON shape.
    # fmt: BSIS Bronze is Delta (one logical table per dataset, schema from the
    # Delta log); master data Bronze is still plain Parquet -- it's an
    # overwrite-each-run reference snapshot, not append-only history, so it
    # never needed Delta's MERGE/schema-enforcement in the first place.
    path = lakehouse_path("bronze", *(path_parts or (source,)), dataset)
    try:
        df = spark.read.format(fmt).load(path) if fmt == "delta" else spark.read.parquet(path)
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
    logger.info("Inspecting bronze/master (%d datasets)", len(MASTER_DATASETS))
    bronze_master = [
        bronze_entry(spark, "master", ds, MASTER_DESCRIPTIONS.get(ds, ""), None, fmt="parquet") for ds in MASTER_DATASETS
    ]

    logger.info("Inspecting silver/DFSS/<domain> (%d datasets)", len(BSIS_DATASETS))
    silver_bsis = [
        delta_entry(spark, "silver", "bsis", ds, *BSIS_DESCRIPTIONS[ds], path_parts=("DFSS", DATASET_DOMAIN[ds]))
        for ds in BSIS_DATASETS
    ]
    logger.info("Inspecting silver/master (%d datasets)", len(MASTER_DATASETS))
    silver_master = [
        delta_entry(spark, "silver", "master", ds, MASTER_DESCRIPTIONS.get(ds, ""), None) for ds in MASTER_DATASETS
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
