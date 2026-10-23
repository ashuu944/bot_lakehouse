#!/bin/sh
# Registers every DFSS/BSIS Bronze Delta table into Trino's delta.bronze
# schema -- same mechanism already used for Silver (delta.silver) and Gold
# (delta.gold), now that bronze_ingestion.py writes Delta instead of plain
# Parquet. This replaces the earlier attempt to expose Bronze via
# OpenMetadata's S3 Datalake connector, which has no concept of a Hive-
# partitioned directory being ONE table -- it listed every reporting_date
# partition file as its own "table" (355 of them for 12 real datasets).
# Going through Trino instead means Bronze is visible the exact same way as
# Silver/Gold: one logical table per dataset, schema read from the Delta log.
#
# Safe to re-run: register_table is a no-op if the table is already
# registered at the same location.
#
# Usage (run on the HOST, not inside a container -- it shells out to
# `docker exec lakehouse-trino` itself, per statement):
#   bash trino/register_bronze_tables.sh

set -e

run() {
  docker exec lakehouse-trino trino --execute "$1"
}

run "CREATE SCHEMA IF NOT EXISTS delta.bronze WITH (location = 's3://lakehouse/bronze/');"

register() {
  dataset="$1"
  run "CALL delta.system.register_table(schema_name => 'bronze', table_name => '${dataset}', table_location => 's3://lakehouse/bronze/DFSS/BSIS/${dataset}');"
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

echo "Done -- delta.bronze now has 12 tables registered."
