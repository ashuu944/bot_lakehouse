"""Named steps for the `structured_tabular` pipeline_type.

Each step has the signature `(df, ctx, **kwargs) -> DataFrame` so the runner
in silver_transform.py can call any of them uniformly from a config entry of
the form `{step: <name>, ...kwargs}`. The actual column-level logic is
unchanged from the original BSIS-specific job -- these are thin, parametrized
wrappers around silver_common.py so the same tested logic now also carries a
name a config can reference.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from silver_common import (
    add_business_key,
    apply_aliases,
    dedupe_keep_latest,
    merge_into_delta,
    normalize_column_names,
    slug_column,
)

from .context import PipelineContext
from .registry import step

logger = logging.getLogger("pipeline_steps.structured")


@step("normalize_columns")
def normalize_columns(df: DataFrame, ctx: PipelineContext, **_) -> DataFrame:
    return normalize_column_names(df)


@step("rename")
def rename(df: DataFrame, ctx: PipelineContext, map: dict[str, str], **_) -> DataFrame:
    for old, new in map.items():
        df = df.withColumnRenamed(old, new)
    return df


@step("join_dimension")
def join_dimension(
    df: DataFrame,
    ctx: PipelineContext,
    lookup: str,
    join_key: str,
    bring_cols: list[str],
    how: str = "left",
    rename: dict[str, str] | None = None,
    **_,
) -> DataFrame:
    """Left-join a Silver reference table, keeping only the columns a dataset
    actually needs from it (e.g. institutions -> category_id/type_id/is_active).
    """
    dim = ctx.read_silver(lookup).select(join_key, *bring_cols)
    for old, new in (rename or {}).items():
        dim = dim.withColumnRenamed(old, new)
    return df.join(dim, on=join_key, how=how)


@step("resolve_dimension")
def resolve_dimension(
    df: DataFrame,
    ctx: PipelineContext,
    source_col: str,
    lookup: str,
    lookup_key: str,
    bring_cols: list[str] | None = None,
    aliases: dict[str, str] | None = None,
    join_type: str = "inner",
    **_,
) -> DataFrame:
    """Normalize a free-text source column, correct known source typos via
    `aliases`, then join a reference table on the normalized text -- an inner
    join both resolves the dimension key AND filters out rows that aren't a
    real match (e.g. BSIS's Public/Private breakdown and summary rows that
    don't correspond to any real sector).
    """
    total_rows = df.count()
    norm_col = f"_{source_col}_norm"

    df = df.withColumn(norm_col, F.trim(F.lower(F.col(source_col))))
    if aliases:
        df = df.withColumn(norm_col, apply_aliases(F.col(norm_col), aliases))

    dim = ctx.read_silver(lookup).withColumn(norm_col, F.trim(F.lower(F.col(lookup_key))))
    dim_cols = [norm_col] + (bring_cols or [c for c in dim.columns if c != norm_col])
    dim = dim.select(*dim_cols)

    df = df.join(dim, on=norm_col, how=join_type).drop(norm_col, source_col)

    if join_type == "inner":
        matched_rows = df.count()
        logger.info(
            "resolve_dimension(%s -> %s): %d/%d rows matched (rest dropped as non-matching rows)",
            source_col, lookup, matched_rows, total_rows,
        )
    return df


@step("pivot_long_to_wide")
def pivot_long_to_wide(
    df: DataFrame,
    ctx: PipelineContext,
    key_cols: list[str],
    line_no_col: str,
    line_desc_col: str,
    value_cols: dict[str, str],
    **_,
) -> DataFrame:
    """Reshape a long/line-item report (one row per line per institution+date)
    into one row per institution+date, one column per line item -- keyed by
    zero-padded line number + slugified description to avoid column-name
    collisions from repeated labels (e.g. "Others (Specify)" under multiple
    parent sections).

    value_cols maps {source_value_column: output_alias}. A single entry
    reproduces the original single-value-per-line-item column naming
    (just the line item key, no suffix); more than one (e.g. monthly + YTD
    amounts) suffixes each output column with its alias, since Spark's
    pivot+agg naming requires it once more than one aggregate is requested.
    """
    df = df.withColumn(
        "_line_item",
        F.concat(F.lpad(F.col(line_no_col).cast("int").cast("string"), 3, "0"), F.lit("_"), slug_column(F.col(line_desc_col))),
    )
    if len(value_cols) == 1:
        (src,) = value_cols
        return df.groupBy(*key_cols).pivot("_line_item").agg(F.first(src))

    aggs = [F.first(src).alias(alias) for src, alias in value_cols.items()]
    return df.groupBy(*key_cols).pivot("_line_item").agg(*aggs)


@step("business_key")
def business_key(df: DataFrame, ctx: PipelineContext, id_prefix: str, key_fields: list[str], **_) -> DataFrame:
    return add_business_key(df, key_fields, id_prefix)


@step("dedupe_keep_latest")
def dedupe_keep_latest_step(df: DataFrame, ctx: PipelineContext, order_col: str = "extraction_ts", **_) -> DataFrame:
    return dedupe_keep_latest(df, "business_key", order_col)


@step("merge_delta")
def merge_delta_step(df: DataFrame, ctx: PipelineContext, out_path: str, **_) -> DataFrame:
    merge_into_delta(ctx.spark, df, out_path, "business_key")
    return df
