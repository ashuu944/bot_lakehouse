#!/bin/sh
# Registers every DFSS/BSIS Silver Delta table into Trino's delta.silver
# schema, same mechanism already used for Gold (delta.gold) -- Trino just
# reads the schema from the Delta log at the given location, nothing is
# copied. Safe to re-run: register_table is a no-op if the table is already
# registered at the same location (CREATE TABLE IF NOT EXISTS semantics).
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

run "CREATE SCHEMA IF NOT EXISTS delta.silver WITH (location = 's3://lakehouse/silver/');"

register() {
  dataset="$1"
  domain="$2"
  run "CALL delta.system.register_table(schema_name => 'silver', table_name => '${dataset}', table_location => 's3://lakehouse/silver/DFSS/${domain}/${dataset}');"
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

echo "Done -- delta.silver now has 12 tables registered."
