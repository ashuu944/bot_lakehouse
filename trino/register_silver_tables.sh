#!/bin/sh
# Registers every DFSS/BSIS Silver Delta table (the 12 BSIS fact datasets
# plus the 11 master/reference datasets) into Trino's delta.silver schema,
# same mechanism already used for Gold (delta.gold) -- Trino just reads the
# schema from the Delta log at the given location, nothing is copied. Safe
# to re-run: register_table() below treats Trino's "Table already exists"
# error as success (register_table itself does NOT no-op on a
# re-registration the way CREATE TABLE IF NOT EXISTS would -- it errors,
# despite what this script used to claim).
#
# Domain mapping mirrors DATASET_DOMAIN in spark/jobs/silver_transform.py --
# keep the two in sync if a dataset's domain ever changes there.
#
# Usage (run on the HOST, not inside a container -- it shells out to
# `docker exec lakehouse-trino` itself, per statement):
#   bash trino/register_silver_tables.sh

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

run "CREATE SCHEMA IF NOT EXISTS delta.silver WITH (location = 's3://lakehouse/silver/');"

register() {
  dataset="$1"
  domain="$2"
  register_table silver "${dataset}" "s3://lakehouse/silver/DFSS/${domain}/${dataset}"
}

register smr_daily_compliance liquidity_risk
register liquidity_compliance liquidity_risk
register lcr liquidity_risk
register nsfr liquidity_risk
register net_open_position market_risk
register sensitivity_market_risk market_risk
register capital_adequacy capital_adequacy
register asset_quality credit_risk
register credit_exposure credit_risk
register balance_sheet financial_statements
register income_statement financial_statements
register earnings financial_statements

# Master/reference data -- lives at silver/DFSS/BSIS/master/<dataset>
# (silver_master_transform.py), not under a domain the way the fact tables
# above are, so it gets its own register function instead of reusing
# register()'s domain-shaped location. Same schema (delta.silver) as the
# fact tables -- no name collision, and the existing OpenMetadata Trino
# connector (already pointed at this whole schema) picks these up for free.
register_master() {
  dataset="$1"
  register_table silver "${dataset}" "s3://lakehouse/silver/DFSS/BSIS/master/${dataset}"
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

echo "Done -- delta.silver now has 23 tables registered (12 BSIS + 11 master)."
