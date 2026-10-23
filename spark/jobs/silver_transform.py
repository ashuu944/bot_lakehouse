"""Phase 3: clean, standardize, and conform Bronze data into Silver (Delta),
generically -- driven entirely by spark/config/sources/**/*.yaml, not by a
per-dataset Python function.

Run silver_master_transform.py FIRST -- most datasets join against
silver/master/institutions (and credit_exposure against silver/master/sectors).

Every dataset here is pipeline_type "structured_tabular", which runs this
fixed skeleton (see run_structured_tabular below):

    normalize_columns -> rename -> extra_steps (dataset-specific, often none)
    -> join_dimensions -> business_key -> dedupe_keep_latest -> merge_delta

The skeleton itself is Python (control flow shouldn't live in YAML); what a
specific dataset needs from it -- its rename map, which reference tables to
join, its business key fields, and any genuinely dataset-specific reshaping
(a pivot, a fuzzy-match-and-filter join) -- is config. See
pipeline_steps/structured.py for what each named step actually does, and
spark/config/sources/DFSS/BSIS.yaml for the 12 BSIS datasets expressed this
way -- 9 of them need nothing beyond the common tail; only credit_exposure,
balance_sheet, and income_statement need an extra_steps entry.

Adding a new pipeline_type (e.g. "unstructured_document" for PDFs/OCR, a
later and separate concern -- very different processing, not a variant of
this one) means adding its own runner function + registering it in
PIPELINE_RUNNERS below, not touching this one.

Usage:
    docker compose exec spark spark-submit /opt/spark-jobs/silver_transform.py
    docker compose exec spark spark-submit /opt/spark-jobs/silver_transform.py --dataset lcr,nsfr
    docker compose exec spark spark-submit /opt/spark-jobs/silver_transform.py --dataset balance_sheet,income_statement,credit_exposure
"""

import argparse
import logging

from pipeline_steps import PipelineContext, get_step
from source_config import build_dataset_index
from spark_session import get_spark, lakehouse_path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("silver_transform")


def parse_args():
    parser = argparse.ArgumentParser(description="Transform Bronze data into Silver")
    parser.add_argument("--dataset", default=None, help="Comma-separated dataset keys (default: all configured)")
    return parser.parse_args()


def run_structured_tabular(ctx: PipelineContext, entry: dict) -> None:
    dataset = entry["dataset"]
    in_path = lakehouse_path("bronze", entry["directorate"], entry["source_system"], dataset)
    out_path = lakehouse_path("silver", entry["directorate"], entry["domain"], dataset)
    logger.info("Transforming %s", dataset)

    df = ctx.spark.read.format("delta").load(in_path)
    df = get_step("normalize_columns")(df, ctx)

    if entry.get("rename"):
        df = get_step("rename")(df, ctx, map=entry["rename"])

    for spec in entry.get("extra_steps", []):
        spec = dict(spec)
        step_name = spec.pop("step")
        df = get_step(step_name)(df, ctx, **spec)

    for join_spec in entry.get("join_dimensions", []):
        df = get_step("join_dimension")(df, ctx, **join_spec)

    bk = entry["business_key"]
    df = get_step("business_key")(df, ctx, id_prefix=bk["id_prefix"], key_fields=bk["key_fields"])
    df = get_step("dedupe_keep_latest")(df, ctx)
    df = get_step("merge_delta")(df, ctx, out_path=out_path)

    logger.info("Upserted %d rows into %s", df.count(), out_path)


PIPELINE_RUNNERS = {
    "structured_tabular": run_structured_tabular,
}


def main():
    args = parse_args()
    dataset_index = build_dataset_index()
    requested = args.dataset.split(",") if args.dataset else sorted(dataset_index)

    unknown = [d for d in requested if d not in dataset_index]
    if unknown:
        raise SystemExit(f"Unknown dataset(s): {', '.join(unknown)}. Available: {', '.join(sorted(dataset_index))}")

    spark = get_spark("silver-transform")
    ctx = PipelineContext(spark=spark)

    for dataset in requested:
        entry = dataset_index[dataset]
        pipeline_type = entry.get("pipeline_type", "structured_tabular")
        try:
            runner = PIPELINE_RUNNERS[pipeline_type]
        except KeyError:
            raise SystemExit(
                f"{dataset}: unknown pipeline_type {pipeline_type!r}. Available: {sorted(PIPELINE_RUNNERS)}"
            ) from None
        runner(ctx, entry)

    spark.stop()


if __name__ == "__main__":
    main()
