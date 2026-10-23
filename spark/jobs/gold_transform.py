"""Phase 4: Silver -> Gold (Delta). Business/analysis-ready marts, one per use case.

Gold is NOT a single "analysis-ready" copy of everything in Silver. Each table
here exists because a specific consumer asked for a specific shape — adding a
new Gold table should always be traceable to a named use case, not built
speculatively. Right now there are two:

  - sector_exposure: institution x sector x month credit exposure, with asset
    quality (performing/non-performing) derived. Grain matches Silver
    credit_exposure already (no aggregation needed) -- this is reshaping +
    derived metrics, not rollup. Feeds: Power BI sector trend charts, and is
    the natural source for a future Neo4j institution-sector exposure graph
    (one row here = one weighted edge).
  - institution_risk_trends: one row per institution x month, with the core
    prudential ratios (LCR, NSFR, capital adequacy) pulled side-by-side from
    three separate Silver tables. Feeds: Power BI per-institution trend
    lines -- the whole point is putting ratios that live in different Silver
    tables into one wide row so a line chart doesn't need three data sources.

Unlike Silver, Gold tables are always fully rebuilt (mode "overwrite"), not
MERGEd. Silver already holds the deduplicated, upserted truth -- Gold is a
pure derivation from it, so recomputing it from scratch each run is simpler
than upserting and just as correct.

Usage:
    docker compose exec spark spark-submit /opt/spark-jobs/gold_transform.py
    docker compose exec spark spark-submit /opt/spark-jobs/gold_transform.py --table sector_exposure
    docker compose exec spark spark-submit /opt/spark-jobs/gold_transform.py --table institution_risk_trends
"""

import argparse
import logging

from pyspark.sql import functions as F

from spark_session import get_spark, lakehouse_path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("gold_transform")

INSTITUTION_COLS = ["institution_code", "institutionname", "report_date", "category_id", "type_id", "institution_is_active"]


def parse_args():
    parser = argparse.ArgumentParser(description="Transform Silver BSIS data into Gold marts")
    parser.add_argument(
        "--table", default=None, help="Comma-separated Gold table(s): sector_exposure, institution_risk_trends (default: both)"
    )
    return parser.parse_args()


def build_sector_exposure(spark) -> None:
    out_path = lakehouse_path("gold", "bsis", "sector_exposure")
    logger.info("Building sector_exposure")

    df = spark.read.format("delta").load(lakehouse_path("silver", "DFSS", "credit_risk", "credit_exposure"))

    total_exposure = F.coalesce(F.col("outstanding_local"), F.lit(0.0)) + F.coalesce(F.col("outstanding_foreign"), F.lit(0.0))
    non_performing = (
        F.coalesce(F.col("substandard_local"), F.lit(0.0))
        + F.coalesce(F.col("substandard_foreign"), F.lit(0.0))
        + F.coalesce(F.col("doubtful_local"), F.lit(0.0))
        + F.coalesce(F.col("doubtful_foreign"), F.lit(0.0))
        + F.coalesce(F.col("loss_local"), F.lit(0.0))
        + F.coalesce(F.col("loss_foreign"), F.lit(0.0))
    )

    df = (
        df.select(
            "institution_code", "institutionname", "report_date",
            "category_id", "type_id", "institution_is_active",
            "sector_code", "sector_name",
            "outstanding_local", "outstanding_foreign",
            "current_local", "current_foreign",
            "substandard_local", "substandard_foreign",
            "doubtful_local", "doubtful_foreign",
            "loss_local", "loss_foreign",
        )
        .withColumn("total_exposure", total_exposure)
        .withColumn("non_performing_exposure", non_performing)
        .withColumn(
            "npl_ratio",
            F.when(total_exposure > 0, non_performing / total_exposure).otherwise(F.lit(None).cast("double")),
        )
    )

    df.write.format("delta").mode("overwrite").save(out_path)
    logger.info("Wrote %d rows to %s", df.count(), out_path)


def build_institution_risk_trends(spark) -> None:
    out_path = lakehouse_path("gold", "bsis", "institution_risk_trends")
    logger.info("Building institution_risk_trends")

    lcr = spark.read.format("delta").load(lakehouse_path("silver", "DFSS", "liquidity_risk", "lcr")).select(
        *INSTITUTION_COLS, F.col("lcr").alias("lcr_ratio"), "liquidity_rating"
    )
    nsfr = spark.read.format("delta").load(lakehouse_path("silver", "DFSS", "liquidity_risk", "nsfr")).select(
        "institution_code", "report_date", F.col("net_stable_funding").alias("nsfr_ratio")
    )
    capital = spark.read.format("delta").load(lakehouse_path("silver", "DFSS", "capital_adequacy", "capital_adequacy")).select(
        "institution_code", "report_date", "tier1_capital_ratio", "total_capital_ratio",
        "leverage_ratio", "capital_adequacy_rating",
    )

    df = (
        lcr.join(nsfr, on=["institution_code", "report_date"], how="outer")
        .join(capital, on=["institution_code", "report_date"], how="outer")
    )

    df.write.format("delta").mode("overwrite").save(out_path)
    logger.info("Wrote %d rows to %s", df.count(), out_path)


BUILDERS = {
    "sector_exposure": build_sector_exposure,
    "institution_risk_trends": build_institution_risk_trends,
}


def main():
    args = parse_args()
    requested = args.table.split(",") if args.table else sorted(BUILDERS)

    unknown = [t for t in requested if t not in BUILDERS]
    if unknown:
        raise SystemExit(f"Unknown table(s): {', '.join(unknown)}. Available: {', '.join(sorted(BUILDERS))}")

    spark = get_spark("gold-transform")
    for table in requested:
        BUILDERS[table](spark)
    spark.stop()


if __name__ == "__main__":
    main()
