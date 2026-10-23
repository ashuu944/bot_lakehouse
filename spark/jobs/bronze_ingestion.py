"""Phase 2: extract source-system datasets into Bronze (Delta), generically.

This job is NOT specific to any one directorate or source system -- which
dataset belongs to which directorate/source system, and which Python module
extracts it, comes entirely from spark/config/sources/**/*.yaml
(source_config.py). Onboarding a new directorate's source system means
adding a config file + an extractor module exposing get_extractor(dataset)/
get_registry_info(), not editing this script.

The extractor module itself (e.g. bsis_extractor) still owns the actual SQL/
extraction logic and per-dataset metadata (requires_date); this job only
orchestrates it against the configured cadence and writes its output to the
lakehouse, adding source/lineage metadata. Bronze is Delta (not plain
Parquet) so a dataset's schema is enforced and readable as one logical table
regardless of how many reporting-date partitions exist underneath it --
important for tools (OpenMetadata, Trino, Power BI) that otherwise have no
reliable way to tell "355 files" apart from "12 datasets".

Usage

── Single date ─────────────────────────────────────────────────────────────
    # one dataset, one date
    docker compose exec spark spark-submit /opt/spark-jobs/bronze_ingestion.py --dataset nsfr --date 2026-09-30

    # several datasets, one date
    docker compose exec spark spark-submit /opt/spark-jobs/bronze_ingestion.py --dataset lcr,nsfr --date 2026-09-30

── Backfill (date range) ────────────────────────────────────────────────────
Each dataset walks its own reporting cadence automatically (daily datasets get
every day, monthly datasets get each period-end date -- see bsis_frequency.py;
this is still BSIS-specific cadence metadata, kept separate from the generic
directorate/source-system config since it's a date-walking concern, not a
transform-shape one).
--end defaults to today if omitted.

    # one dataset, from a start date to today
    docker compose exec spark spark-submit /opt/spark-jobs/bronze_ingestion.py --dataset nsfr --start 2026-06-01

    # one dataset, explicit start/end
    docker compose exec spark spark-submit /opt/spark-jobs/bronze_ingestion.py --dataset lcr,nsfr --start 2026-06-01 --end 2026-06-30

── Notes ─────────────────────────────────────────────────────────────────────
- Omitting --dataset entirely processes every dataset declared across all
  source configs.
- --batch-id lets you tag a run explicitly; otherwise a uuid4 is generated.
- A long backfill can take a while (hundreds of source round-trips) -- add
  `> logfile.txt 2>&1` to capture output if running unattended.
"""

import argparse
import importlib
import logging
import uuid
from datetime import date, datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, ShortType

from bsis_frequency import frequency_of, generate_date_range
from source_config import build_dataset_index
from spark_session import get_spark, lakehouse_path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("bronze_ingestion")

_extractor_modules: dict[str, object] = {}


def _extractor_module(module_name: str):
    if module_name not in _extractor_modules:
        _extractor_modules[module_name] = importlib.import_module(module_name)
    return _extractor_modules[module_name]


def parse_args():
    parser = argparse.ArgumentParser(description="Extract source-system datasets into Bronze")
    parser.add_argument("--dataset", default=None, help="Comma-separated dataset keys (default: all configured)")
    parser.add_argument("--date", default=None, help="Single reporting date YYYY-MM-DD")
    parser.add_argument("--start", default=None, help="Backfill: start date YYYY-MM-DD (inclusive)")
    parser.add_argument("--end", default=None, help="Backfill: end date YYYY-MM-DD (inclusive, default: today)")
    parser.add_argument("--batch-id", default=None, help="Ingestion batch id (default: generated uuid4)")
    args = parser.parse_args()

    if args.date and args.start:
        parser.error("--date and --start are mutually exclusive")
    if args.start and not args.end:
        args.end = date.today().isoformat()
    return args


def extract_and_write(spark, entry: dict, report_date: str | None, batch_id: str, extraction_ts: datetime) -> None:
    dataset = entry["dataset"]
    directorate = entry["directorate"]
    source_system = entry["source_system"]
    logger.info("Extracting %s/%s/%s (date=%s)", directorate, source_system, dataset, report_date or "static")

    extractor = _extractor_module(entry["extractor_module"])
    try:
        result = extractor.get_extractor(dataset).extract_full(report_date)
    except Exception:
        logger.exception("Extraction failed for %s date=%s, skipping", dataset, report_date)
        return

    logger.info("%s: %s", dataset, result.quality_report.summary())

    pdf = result.data
    if pdf.empty:
        logger.warning("No rows for %s date=%s, skipping write", dataset, report_date)
        return

    sdf = spark.createDataFrame(pdf)

    # Stabilize numeric types across independent per-date writes. pandas/Spark
    # infer LongType on a date where a ratio/rating column's values all happen
    # to be whole numbers, and DoubleType on a date where they don't -- for
    # the SAME logical column. Different partitions of one dataset then
    # physically disagree on type, which Delta's schema enforcement would
    # reject outright (Parquet would just silently write it, and break on
    # read instead). Widening every integer-ish numeric column to double here
    # makes the schema stable across all dates; it changes representation,
    # not values.
    for field in sdf.schema.fields:
        if isinstance(field.dataType, (LongType, IntegerType, ShortType)):
            sdf = sdf.withColumn(field.name, sdf[field.name].cast(DoubleType()))

    sdf = (
        sdf.withColumn("source_system", F.lit(source_system.lower()))
        .withColumn("source_dataset", F.lit(dataset))
        .withColumn(
            "reporting_date",
            F.to_date(F.lit(report_date)) if report_date else F.lit(None).cast("date"),
        )
        .withColumn("extraction_ts", F.lit(extraction_ts))
        .withColumn("ingestion_batch_id", F.lit(batch_id))
    )

    # Namespaced by directorate/source system so Bronze doesn't collide when
    # a second source system lands a dataset with the same name.
    out_path = lakehouse_path("bronze", directorate, source_system, dataset)
    logger.info("Writing %d rows to %s", sdf.count(), out_path)
    writer = sdf.coalesce(1).write.mode("append").format("delta")
    if report_date:
        # Partition by date so each reporting date is its own folder
        # (visible in the MinIO console, and prunable on read) rather than
        # relying solely on the reporting_date column inside the files.
        writer = writer.partitionBy("reporting_date")
    writer.save(out_path)


def main():
    args = parse_args()
    batch_id = args.batch_id or str(uuid.uuid4())
    extraction_ts = datetime.now(timezone.utc)

    dataset_index = build_dataset_index()
    requested = args.dataset.split(",") if args.dataset else sorted(dataset_index)

    unknown = [d for d in requested if d not in dataset_index]
    if unknown:
        raise SystemExit(f"Unknown dataset(s): {', '.join(unknown)}. Available: {', '.join(sorted(dataset_index))}")

    spark = get_spark("bronze-ingestion")
    logger.info("Batch %s starting for datasets: %s", batch_id, ", ".join(requested))

    for dataset in requested:
        entry = dataset_index[dataset]
        extractor = _extractor_module(entry["extractor_module"])
        requires_date = {info["dataset"]: info["requires_date"] for info in extractor.get_registry_info()}.get(dataset)
        if requires_date is None:
            logger.warning("Skipping %s: not found in %s's own registry", dataset, entry["extractor_module"])
            continue

        if not requires_date:
            extract_and_write(spark, entry, None, batch_id, extraction_ts)
            continue

        if args.date:
            extract_and_write(spark, entry, args.date, batch_id, extraction_ts)
            continue

        if args.start:
            if dataset == "deleted_returns":
                logger.warning(
                    "Skipping %s in a date range: it's an audit/high-water-mark feed, "
                    "not a per-date report (pass --date explicitly if you need one run)",
                    dataset,
                )
                continue
            freq = frequency_of(dataset)
            if freq is None:
                logger.warning("Skipping %s: no known reporting cadence for range extraction", dataset)
                continue
            dates = generate_date_range(freq, args.start, args.end)
            logger.info("%s (%s): walking %d date(s) from %s to %s", dataset, freq, len(dates), args.start, args.end)
            for report_date in dates:
                extract_and_write(spark, entry, report_date.isoformat(), batch_id, extraction_ts)
            continue

        logger.warning("Skipping %s: requires --date or --start/--end", dataset)

    spark.stop()
    logger.info("Batch %s complete", batch_id)


if __name__ == "__main__":
    main()
