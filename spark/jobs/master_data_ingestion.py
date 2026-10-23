"""Phase 2: land reference/master data CSVs in Bronze.

These are reference/dimension tables (institutions, sectors, risk types,
regulatory thresholds, ...) used later to enrich and join against the BSIS
Bronze/Silver data — they aren't BSIS data themselves, just another source
landed the same generic way.

Unlike BSIS (append — each reporting date is a distinct historical fact),
each file here is written with mode "overwrite": there's no date dimension,
and a run represents the current state of the reference file, not a new
event.

Usage (run from the project root, containers already up via
`docker compose up -d minio spark`; requires MASTER_DATA_PATH in .env):

    docker compose exec spark spark-submit /opt/spark-jobs/master_data_ingestion.py

No arguments — it always processes every CSV under MASTER_DATA_PATH and
overwrites all 11 bronze/master/<dataset>/ tables in one run.
"""

import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from spark_session import get_spark, lakehouse_path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("master_data_ingestion")

MASTER_DATA_DIR = Path("/opt/master-data")


def main():
    batch_id = str(uuid.uuid4())
    extraction_ts = datetime.now(timezone.utc)

    csv_files = sorted(MASTER_DATA_DIR.glob("*.csv"))
    if not csv_files:
        raise SystemExit(f"No CSV files found under {MASTER_DATA_DIR}")

    spark = get_spark("master-data-ingestion")
    logger.info("Batch %s starting for %d master datasets", batch_id, len(csv_files))

    for csv_path in csv_files:
        dataset = csv_path.stem
        logger.info("Reading %s", csv_path)

        sdf = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(f"file://{csv_path}")
            .withColumn("source_system", F.lit("master_data"))
            .withColumn("source_dataset", F.lit(dataset))
            .withColumn("reporting_date", F.lit(None).cast("date"))
            .withColumn("extraction_ts", F.lit(extraction_ts))
            .withColumn("ingestion_batch_id", F.lit(batch_id))
        )

        out_path = lakehouse_path("bronze", "master", dataset)
        logger.info("Writing %d rows to %s", sdf.count(), out_path)
        sdf.coalesce(1).write.mode("overwrite").format("parquet").save(out_path)

    spark.stop()
    logger.info("Batch %s complete", batch_id)


if __name__ == "__main__":
    main()
