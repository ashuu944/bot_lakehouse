#!/bin/sh
# Registers every DFSS/BSIS Bronze Delta table (the 12 BSIS fact datasets
# plus the 11 master/reference datasets) into Trino's delta.bronze schema --
# same mechanism already used for Silver (delta.silver) and Gold (delta.gold),
# now that bronze_ingestion.py/master_data_ingestion.py write Delta instead of
# plain Parquet. This replaces the earlier attempt to expose Bronze via
# OpenMetadata's S3 Datalake connector, which has no concept of a Hive-
# partitioned directory being ONE table -- it listed every reporting_date
# partition file as its own "table" (355 of them for 12 real datasets).
# Going through Trino instead means Bronze is visible the exact same way as
# Silver/Gold: one logical table per dataset, schema read from the Delta log.
#
# Safe to re-run: register_table() below treats Trino's "Table already
# exists" error as success (register_table itself does NOT no-op on a
# re-registration the way CREATE TABLE IF NOT EXISTS would -- it errors,
# despite what this script used to claim).
#
# Usage (run on the HOST, not inside a container -- it shells out to
# `docker exec lakehouse-trino` itself, per statement):
#   bash trino/register_bronze_tables.sh

set -e

run() {
  docker exec lakehouse-trino trino --execute "$1"
}

register_table() {
  schema="$1"
  dataset="$2"
  location="$3"
  output=$(docker exec lakehouse-trino trino --execute \
    "CALL delta.system.register_table(schema_name => '${schema}', table_name => '${dataset}', table_location => '${location}');" 2>&1) && return 0
  case "$output" in
    *"Table already exists"*) return 0 ;;
    *) echo "$output" >&2; return 1 ;;
  esac
}

run "CREATE SCHEMA IF NOT EXISTS delta.bronze WITH (location = 's3://lakehouse/bronze/');"

register() {
  dataset="$1"
  register_table bronze "${dataset}" "s3://lakehouse/bronze/DFSS/BSIS/${dataset}"
}

register smr_daily_compliance
register liquidity_compliance
register net_open_position
register lcr
register nsfr
register capital_adequacy
register sensitivity_market_risk
register asset_quality
register credit_exposure
register balance_sheet
register earnings
register income_statement

# Master/reference data (institutions, sectors, ...) -- master_data_ingestion.py
# writes these as Delta too (moved off plain Parquet specifically so this
# registration could happen), nested under DFSS/BSIS since it's master data
# OF BSIS, not a separate source. No name collision with the 12 datasets
# above -- registered into the same delta.bronze schema so the existing
# OpenMetadata Trino connector (already pointed at this whole schema) picks
# them up with no config change.
register_master() {
  dataset="$1"
  register_table bronze "${dataset}" "s3://lakehouse/bronze/DFSS/BSIS/master/${dataset}"
}

register_master institutions
register_master institution_categories
register_master institution_types
register_master sectors
register_master risk_types
register_master regulatory_thresholds
register_master reporting_frequencies
register_master reporting_obligations
register_master dataset_types
register_master camels_rating_criteria
register_master holidays

echo "Done -- delta.bronze now has 23 tables registered (12 BSIS + 11 master)."
