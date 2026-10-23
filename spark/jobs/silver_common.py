"""Shared helpers for Bronze -> Silver transforms.

Column normalization and the business_key/surrogate-id pattern mirror the
existing production pipeline's transform layer
(bank_profiling_risk_analysis/data_pipeline/transform/), adapted for Spark/
Delta instead of pandas/CSV.
"""

from __future__ import annotations

import re

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def apply_aliases(col: F.Column, aliases: dict[str, str]) -> F.Column:
    """Substitute known-wrong values in a (normalized) column via a lookup dict.

    For source-text typos/variants that need correcting before a join —
    e.g. BSIS's own data spelling a sector name differently from the master.
    """
    result = col
    for wrong, right in aliases.items():
        result = F.when(col == wrong, F.lit(right)).otherwise(result)
    return result


def slug_column(col: F.Column) -> F.Column:
    """Lowercase/snake_case a Spark string column's values (for building pivot column names)."""
    return F.regexp_replace(F.regexp_replace(F.trim(F.lower(col)), r"[^a-z0-9]+", "_"), r"(^_+|_+$)", "")


def normalize_column_names(df: DataFrame) -> DataFrame:
    """Lowercase, snake_case column names (mirrors the existing pipeline's convention)."""
    for col in df.columns:
        new_col = re.sub(r"[^a-z0-9]+", "_", col.strip().lower()).strip("_")
        if new_col != col:
            df = df.withColumnRenamed(col, new_col)
    return df


def add_business_key(
    df: DataFrame,
    key_fields: list[str],
    id_prefix: str,
    business_key_col: str = "business_key",
    id_col: str = "silver_id",
) -> DataFrame:
    """Add a pipe-joined business_key and a deterministic surrogate id.

    The surrogate id is a dataset-prefixed hash of the business key (not a
    random uuid) so re-processing the same logical row always yields the
    same id — required for the MERGE upsert in merge_into_delta to work.
    """
    df = df.withColumn(
        business_key_col,
        F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in key_fields]),
    )
    df = df.withColumn(
        id_col,
        F.concat(F.lit(f"{id_prefix}_"), F.upper(F.substring(F.sha2(F.col(business_key_col), 256), 1, 16))),
    )
    return df


def dedupe_keep_latest(df: DataFrame, business_key_col: str, order_col: str = "extraction_ts") -> DataFrame:
    """Keep only the most recent row per business_key (by order_col).

    Needed because re-running Bronze ingestion for a date already present
    appends another copy — Silver must collapse that back to one row.
    """
    w = Window.partitionBy(business_key_col).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def merge_into_delta(spark: SparkSession, df: DataFrame, target_path: str, business_key_col: str) -> None:
    """Upsert df into the Delta table at target_path, keyed by business_key_col.

    Creates the table on first run. This is why Silver is Delta and not
    Parquet: Bronze is append-only raw landing (Parquet is enough), but
    Silver needs idempotent upserts when the same business key is
    reprocessed — exactly what plain Parquet can't do and Delta's MERGE can.
    """
    if DeltaTable.isDeltaTable(spark, target_path):
        target = DeltaTable.forPath(spark, target_path)
        (
            target.alias("t")
            .merge(df.alias("s"), f"t.{business_key_col} = s.{business_key_col}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").save(target_path)
