"""Phase 3: clean/standardize Bronze master data into Silver (Delta).

Thin pass-through — the master CSVs already use lowercase snake_case headers
and have no date dimension, so there isn't much to clean. This mainly:
normalizes column names defensively, trims strings, and dedupes. Bronze
master is Delta too now (master_data_ingestion.py), so this is Delta-to-Delta
-- kept as its own step regardless, same as before, since dropDuplicates()
and coalesce(1) are still real transforms, not a pure format change.

Run this BEFORE silver_transform.py — it reads silver/DFSS/BSIS/master/institutions.

Usage:
    docker compose exec spark spark-submit /opt/spark-jobs/silver_master_transform.py
"""

import logging

from pyspark.sql import functions as F

from silver_common import normalize_column_names
from spark_session import get_spark, lakehouse_path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("silver_master_transform")

MASTER_DATASETS = [
    "institutions",
    "institution_categories",
    "institution_types",
    "sectors",
    "risk_types",
    "regulatory_thresholds",
    "reporting_frequencies",
    "reporting_obligations",
    "dataset_types",
    "camels_rating_criteria",
    "holidays",
]


def main():
    spark = get_spark("silver-master-transform")

    for dataset in MASTER_DATASETS:
        in_path = lakehouse_path("bronze", "DFSS", "BSIS", "master", dataset)
        out_path = lakehouse_path("silver", "DFSS", "BSIS", "master", dataset)
        logger.info("Transforming %s", dataset)

        df = normalize_column_names(spark.read.format("delta").load(in_path))
        for field in df.schema.fields:
            if field.dataType.typeName() == "string":
                df = df.withColumn(field.name, F.trim(F.col(field.name)))
        df = df.dropDuplicates()

        df.coalesce(1).write.mode("overwrite").format("delta").save(out_path)
        logger.info("Wrote %d rows to %s", df.count(), out_path)

    spark.stop()


if __name__ == "__main__":
    main()
