# bot_lakehouse

On-premise lakehouse proof of concept: MinIO (bronze/silver/gold) + Spark + Delta Lake,
orchestrated with Airflow, serving PostgreSQL / Power BI / Neo4j downstream.

BSIS (via the existing `bsis_extractor` library) is the first test source; the
architecture is meant to generalize to other banking sources (HRMS, CRD, TISS, ...)
without redesign.

## Status

- [x] **Phase 1** — Docker, MinIO, Spark, Delta Lake. Verified Spark can write/read a
      Delta table stored in MinIO (`spark/jobs/verify_delta_minio.py`).
- [x] **Phase 2** — Integrate `bsis_extractor`, write to Bronze. Also lands reference/
      master data (institutions, sectors, risk types, ...) in Bronze under its own
      `master_data` source — see `spark/jobs/master_data_ingestion.py`. Full backfill
      landed: 12 BSIS fact datasets (3 daily, 9 monthly) from 2026-06-01 to today,
      348 date/dataset writes, 0 failures, plus 11 master/reference tables.
      `institutions`/`industry` deliberately excluded — stub SQL in `bsis_extractor`,
      and master data already covers institution info, so not needed here.
- [x] **Phase 3** — Bronze → Silver (Spark + Delta). All 12 BSIS datasets plus
      all 11 master tables — see `spark/jobs/silver_transform.py` /
      `silver_master_transform.py`. `credit_exposure`, `balance_sheet`,
      `income_statement` come back from Oracle in long/pivoted format (one row
      per line-item) and get an actual pivot (see Silver layer section below).
      Verified on real data: institution/sector enrichment joins, unique
      business keys, Delta MERGE upsert, pivoted column counts and values all
      spot-checked correct.
- [x] **Phase 4** — Silver → Gold (Spark + Delta). Two marts so far, each tied
      to a named use case (Gold tables are added on demand, not speculatively
      — see "Designing Gold" below): `sector_exposure` (institution x sector x
      month credit exposure + derived NPL ratio) and `institution_risk_trends`
      (institution x month, LCR/NSFR/capital-adequacy ratios side by side) —
      see `spark/jobs/gold_transform.py`.
- [ ] Phase 5 — Airflow orchestration.
- [x] **Serving layer** — two query engines in front of Gold, no Postgres
      copy: **Trino** (architecturally the right choice — see "Serving Gold
      via Trino" below) and **Spark Thrift Server** (backup path, since
      Power BI's Presto connector needs a paid Simba ODBC driver unless
      sourced free from Starburst — see "Serving Gold via Spark Thrift
      Server" below). Power BI connects to whichever driver you can get
      installed. Neo4j will instead get a batch loader (Gold is already
      graph-edge-shaped for `sector_exposure`), not live queries.
- [ ] Phase 7 — Neo4j downstream graph load.
- [ ] Phase 8 — Power BI consumption (via Trino or Spark Thrift Server — see below).
- [x] **Data portal** — a small static site (`catalog/`, served by the
      `catalog-portal` nginx container) listing every Bronze/Silver/Gold
      table with its schema, row count, date coverage, institution coverage,
      and what it's for — for demoing "what data exists" without MinIO
      credentials or a Parquet/Delta reader. See "Data portal" below.
- Deferred: PostgreSQL downstream load. Not needed now that Power BI reads
  Gold straight from Trino; revisit only if a consumer specifically needs a
  plain SQL database rather than query-on-read Delta.

## Setup

1. Copy `.env.example` to `.env` and set your own MinIO credentials.
2. Start MinIO and create the `lakehouse` bucket (bronze/silver/gold prefixes):
   ```
   docker compose up -d minio minio-init
   ```
3. Build and start the Spark container:
   ```
   docker compose build spark
   docker compose up -d spark
   ```
4. Verify Spark can read/write Delta tables in MinIO:
   ```
   docker compose exec spark spark-submit /opt/spark-jobs/verify_delta_minio.py
   ```
   Expect `PHASE 1 OK: wrote and read back 3 rows via Delta on MinIO.` at the end.
5. Extract BSIS datasets into Bronze (requires `DB_USER`/`DB_PASSWORD`/`DB_DSN` in `.env`):
   ```
   docker compose exec spark spark-submit /opt/spark-jobs/bronze_ingestion.py --date 2026-02-28

   # backfill a range — each dataset walks at its own reporting cadence
   # (daily datasets get every day; monthly datasets get each period-end date)
   docker compose exec spark spark-submit /opt/spark-jobs/bronze_ingestion.py --start 2026-06-01
   ```
6. Land reference/master data CSVs into Bronze (requires `MASTER_DATA_PATH` in `.env`):
   ```
   docker compose exec spark spark-submit /opt/spark-jobs/master_data_ingestion.py
   ```
7. Transform Bronze → Silver (master data first — BSIS Silver reads
   `silver/master/institutions` and `silver/master/sectors` for the
   enrichment joins):
   ```
   docker compose exec spark spark-submit /opt/spark-jobs/silver_master_transform.py
   docker compose exec spark spark-submit /opt/spark-jobs/silver_transform.py
   ```
   `--dataset` works here too, e.g. `--dataset lcr,nsfr` or
   `--dataset balance_sheet,income_statement,credit_exposure`.
8. Transform Silver → Gold:
   ```
   docker compose exec spark spark-submit /opt/spark-jobs/gold_transform.py
   ```
   `--table sector_exposure` or `--table institution_risk_trends` to build just one.
9. Start Trino and attach the Gold tables (one-time per table — `register_table`
   just points Trino at a Delta table Spark already wrote; it doesn't copy
   anything):
   ```
   docker compose up -d trino
   docker compose exec trino trino --execute "CREATE SCHEMA IF NOT EXISTS delta.gold WITH (location = 's3://lakehouse/gold/bsis/');"
   docker compose exec trino trino --execute "CALL delta.system.register_table(schema_name => 'gold', table_name => 'sector_exposure', table_location => 's3://lakehouse/gold/bsis/sector_exposure');"
   docker compose exec trino trino --execute "CALL delta.system.register_table(schema_name => 'gold', table_name => 'institution_risk_trends', table_location => 's3://lakehouse/gold/bsis/institution_risk_trends');"
   ```
   Re-run `register_table` any time `gold_transform.py` adds a brand new table
   (not needed when it just overwrites an existing one — Trino reads the
   current Delta log on every query, no refresh step).
10. Build/refresh the data portal (safe to re-run any time after a Bronze/
    Silver/Gold job — it only reads, never writes to the lakehouse):
    ```
    docker compose up -d catalog-portal
    docker compose exec spark spark-submit /opt/spark-jobs/catalog_builder.py
    ```
    Open http://localhost:8890 and refresh the page.
11. (Alternative to Trino for Power BI) Start Spark Thrift Server — registers
    the Gold tables into its own local Hive metastore, then starts listening:
    ```
    docker compose up -d spark-thriftserver
    ```
    One-time sanity check from inside the container:
    ```
    docker compose exec spark-thriftserver /opt/bitnami/spark/bin/beeline -u jdbc:hive2://localhost:10000 -n admin -e "SHOW TABLES;"
    ```
    Re-run `register_gold_tables.py` (same command `docker compose exec
    spark-thriftserver spark-submit /opt/spark-jobs/register_gold_tables.py`,
    needs the container already running) any time `gold_transform.py` adds a
    brand new table — not needed for an existing table being overwritten.

MinIO console: http://localhost:9001 (credentials from `.env`).
Trino: http://localhost:8880 (no auth — POC only).
Spark Thrift Server: JDBC/ODBC on localhost:8810 (no auth — POC only).
Data portal: http://localhost:8890.

## Bronze layer sources

- `bronze/DFSS/BSIS/<dataset>/reporting_date=YYYY-MM-DD/` — from `bsis_extractor`,
  **append**, partitioned by `reporting_date` (each date is its own folder —
  visible directly in the MinIO console, and lets Spark skip irrelevant dates
  on read instead of scanning everything). Columns: the extractor's own raw
  Oracle column names (uppercase, unrenamed — e.g. `INSTITUTIONCODE`,
  `DATEVAL`), plus `source_system`, `source_dataset`, `extraction_ts`,
  `ingestion_batch_id`. `DFSS` (Directorate of Financial Sector Supervision)
  / `BSIS` is a `<directorate>/<source>` namespace, not a special case — a
  second source system under the same directorate (e.g. RTSIS) or a dataset
  from a different directorate entirely lands under its own
  `bronze/<DIRECTORATE>/<source>/` prefix without touching this one.
- `bronze/master/<dataset>/` — reference/dimension CSVs (institutions, sectors,
  risk types, regulatory thresholds, CAMELS criteria, ...) from
  `MASTER_DATA_PATH`, **overwrite** each run (no date dimension — a run is the
  current state of the file, not a new event). Same lineage columns as above,
  with `reporting_date` always null.

Both are landed with minimal transformation, per the medallion pattern — the
actual join between BSIS facts and master/reference data happens in Silver,
not here.

### Dataset reporting cadence

BSIS datasets don't all report on the same cadence — `spark/jobs/bsis_frequency.py`
mirrors the `reporting_frequency` field from the existing production pipeline's
`source_config.yaml` so a `--start`/`--end` backfill walks the right dates per
dataset (daily vs. each month's period-end), instead of e.g. querying a monthly
dataset once per calendar day:

- **Daily**: `smr_daily_compliance`, `liquidity_compliance`, `net_open_position`
- **Monthly**: `lcr`, `nsfr`, `capital_adequacy`, `sensitivity_market_risk`,
  `asset_quality`, `credit_exposure`, `balance_sheet`, `earnings`, `income_statement`
- **No date**: `institutions`, `industry` — currently **stub SQL** in
  `bsis_extractor` (`-- TODO: paste SQL query here`), so extracting either
  fails with `ORA-00900: invalid SQL statement`. Not a lakehouse-side bug —
  needs fixing in the `bsis_extractor` repo itself. All 13 dated fact datasets
  have real SQL and work.
- **Excluded from range backfill**: `deleted_returns` — an audit/high-water-mark
  feed (BSIS returns rows with `deleted_date >= date`), not a per-date report.

## Silver layer

Silver is grouped by `<directorate>/<domain>` (`silver/DFSS/liquidity_risk/lcr`,
`silver/DFSS/credit_risk/credit_exposure`, ...) rather than by source system —
Silver's job is to stop being "whatever BSIS called it" and become "what this
actually is", so a dataset's domain here mirrors the `risk_type` taxonomy
already used by the existing BOT risk-profiling pipeline
(`bank_profiling_risk_analysis`), not a new invention. `DATASET_DOMAIN` in
`silver_transform.py` is the source of truth for the mapping; see it there
before adding a new dataset rather than guessing a domain name here.

`spark/jobs/silver_common.py` has the shared logic; `silver_master_transform.py`
and `silver_transform.py` use it. Patterns (column normalization, business_key/
surrogate-id composition, per-dataset key fields) are deliberately mirrored from
the existing production pipeline's `transform/` layer
(`bank_profiling_risk_analysis/data_pipeline/transform/transform_specs.py`),
adapted for Spark/Delta instead of pandas/CSV. One simplification over that
pipeline: it fuzzy-matches institution *names* (with a hand-maintained alias
table) because its Excel/CSV sources are inconsistent; `bsis_extractor` already
returns `INSTITUTIONCODE` directly, matching `institutions.csv`'s
`institution_code` exactly, so Silver does a plain exact-match join instead.

Per dataset: normalize column names → rename `institutioncode`/`dateval` to
`institution_code`/`report_date` → left-join Silver institutions (brings in
`category_id`/`type_id`/`institution_is_active`) → build `business_key` (pipe-
joined natural key) + a deterministic `silver_id` (dataset-prefixed hash, not a
random uuid — reprocessing the same row always yields the same id) → keep only
the latest row per `business_key` by `extraction_ts` → **MERGE** into the Silver
Delta table.

This is deliberately where Delta's MERGE gets used for the first time in this
project. Bronze is Parquet + append (immutable raw landing, nothing to upsert).
Silver is Delta + MERGE because the same business key can legitimately be
reprocessed (e.g. re-running a backfill for an already-loaded date) and must
collapse back to one row, not duplicate — exactly what plain Parquet can't do.

### The 3 pivoted datasets

`credit_exposure`, `balance_sheet`, `income_statement` come back from Oracle
as one row per line-item, not one row per institution+date like the other 9.
Checked against the existing production pipeline's `pivot_extractors.py` for
reference (that module parses messy Excel/RPA exports — not reusable code,
since our data comes cleanly from `bsis_extractor`'s SQL — but it confirmed
the shape):

- **`credit_exposure`** turned out not to need a real pivot. Its `PARTICULARS`
  column alternates between the sector name (already pre-summed by Oracle
  across that sector's `Public`/`Private` sub-rows) and the `Public`/`Private`
  breakdown rows themselves, plus a few portfolio-level summary rows (`Total -
  Public`, `Provision Amount`, ...). Inner-joining `PARTICULARS` against the
  Silver `sectors` master does both jobs at once: anything that isn't a real
  sector name simply fails to match and is dropped, while anything that does
  match gets `sector_id`/`sector_code` enrichment for free. One sector
  (`Electicity`) is a genuine typo in BSIS's own data vs. the master's
  `Electricity` — handled via a one-entry alias map
  (`SECTOR_NAME_ALIASES` in `silver_transform.py`), the same pattern the
  existing pipeline uses for institution name mismatches
  (`INSTITUTION_NAME_ALIASES` in `transform_utils.py`).
- **`balance_sheet`** / **`income_statement`** are genuine pivots: grouped by
  institution+date and pivoted on a line-item key (zero-padded line number +
  slugified description, e.g. `007_banks_in_tanzania` — avoids column-name
  collisions from repeated labels like "Others (Specify)" appearing under
  multiple parent sections). `income_statement` has two value columns
  (monthly, YTD), so each line item produces two output columns
  (`001_interest_income_monthly`, `001_interest_income_ytd`).

**Bronze bug found and fixed while building this**: pandas/Spark infer
`LongType` for a numeric column on a date where every value happens to be a
whole number, and `DoubleType` on a date where it doesn't — for the *same*
logical column (e.g. a rating/ratio). Since each date is written in its own
`spark-submit` run, different `reporting_date` partitions of one dataset
ended up physically disagreeing on type, which breaks reading the dataset as
a whole (`SchemaColumnConvertNotSupportedException`). Fixed in
`bronze_ingestion.py`: every integer-typed column is widened to `double`
before writing, for every dataset, so all partitions agree. Bronze was
cleared and fully re-backfilled after the fix.

## Gold layer

### Designing Gold: demand-driven, not speculative

Silver is built ahead of demand — it has to be complete and clean regardless
of who eventually uses it, which is why all 12 BSIS datasets were finished in
Silver before any Gold table existed. **Gold is the opposite: it's built on
demand, one mart at a time, only once a real consumer and a real question
exist.** There is no "finish Gold" the way there's a "finish Silver" — Gold
simply grows a new table each time a new analysis is confirmed. Don't
pre-build a Gold table for every Silver dataset "just in case"; if nobody has
asked for it yet, it doesn't exist yet, and that's correct, not a gap.

The reason Gold exists at all instead of pointing consumers at Silver
directly: it does the aggregation/join/reshaping work **once**, in one place,
so three different downstream tools don't each reimplement the same logic,
and so consumers are insulated from Silver's schema changing under them.

Two marts exist so far (`spark/jobs/gold_transform.py`), each tracing back to
a specific need discussed for this POC:

- **`sector_exposure`** — one row per institution x sector x month, reshaped
  from Silver `credit_exposure` with two derived columns added
  (`total_exposure`, `non_performing_exposure` → `npl_ratio`, from the
  current/substandard/doubtful/loss breakdown BSIS already classifies).
  No aggregation needed — Silver is already at this grain. This is edge-
  shaped (institution → sector, weighted by exposure), which is exactly what
  a future Neo4j loader needs: one row = one graph edge.
- **`institution_risk_trends`** — one row per institution x month, with LCR,
  NSFR, and capital-adequacy ratios pulled side-by-side from three separate
  Silver tables via an outer join on `institution_code`+`report_date`. Built
  specifically so a Power BI line chart "ratio trend per institution" has one
  data source instead of three.

Unlike Silver, Gold tables are fully rebuilt (`mode("overwrite")`) every run,
never MERGEd. Silver already holds the deduplicated, upserted truth, so Gold
is a pure, stateless derivation from it — recomputing from scratch is simpler
than upserting and exactly as correct.

### Serving Gold via Trino

Power BI can't safely read Delta tables as plain Parquet/S3 folders — it has
no notion of the Delta transaction log, so on a table that's had MERGE
upserts (any Silver table) it would double-count superseded files. It needs a
query engine in front that understands Delta. Two realistic options were
considered:

- **Spark Thrift Server** — zero new containers (reuses the existing Spark
  image), but runs inside the same JVM used for batch Bronze/Silver/Gold
  jobs, so a live BI query could contend with a scheduled ingestion run.
- **Trino** (chosen) — a separate process, built specifically for this
  pattern (interactive SQL over a lakehouse for BI tools), with native Delta
  Lake and Power BI/Presto connector support. One more container, but the
  architecturally correct one.

Trino's Delta Lake connector needs *some* metastore to track schema → table →
location mappings (it doesn't discover Delta tables by path alone, unlike
Spark). Rather than standing up a separate Hive Metastore service plus its
own backing database, `trino/catalog/delta.properties` uses Trino's built-in
**file metastore** (`hive.metastore=file`) — it stores that mapping as JSON
under a location Trino itself manages (`s3://lakehouse/_trino_metastore/`, a
prefix separate from `bronze/silver/gold`). No extra service.

Gold tables are attached with `CALL delta.system.register_table(...)`
pointing at the Delta path Spark already wrote — Trino reads the schema
straight from the Delta log, so nothing is redefined or copied. This needs
`delta.register-table-procedure.enabled=true` (off by default). Re-running
`gold_transform.py` later (overwriting the same table) needs no Trino-side
action; registration is only needed once per table, the first time it's
created.

Note Trino's S3 integration uses the `s3://` URI scheme for its own config
(catalog dir, `CREATE SCHEMA ... LOCATION`), not `s3a://` like Spark/Hadoop —
both point at the same MinIO bucket, just via different filesystem clients.

**Power BI connection** (from the Windows machine running Power BI Desktop):
1. Install the Simba Presto ODBC driver (Trino is wire-compatible with
   Presto; see Trino's docs for the client driver download) and the Power BI
   **Presto** connector — no driver install needed if using Power BI's
   native Presto connector in newer versions, otherwise install the driver
   first.
2. Get Data → More → Database → Presto. Server: `localhost:8880`. Catalog:
   `delta`. No authentication (POC only — Trino's `default` security has no
   auth configured, don't expose port 8880 beyond localhost/this network).
3. Navigate to schema `gold`, import `sector_exposure` and/or
   `institution_risk_trends`.

### Serving Gold via Spark Thrift Server (backup path for Power BI)

Power BI's **Presto** connector (used to reach Trino) needs a Presto-
compatible ODBC driver installed locally, and Simba Technologies sells that
driver directly as a paid product — the free way to get it is via Starburst's
own driver download (same underlying driver, free registration, since
Starburst makes Trino). If that's inconvenient, Power BI's native **Spark**
connector plus the **Simba Spark ODBC driver** (distributed free, no
paywall, to support the open Apache Spark ecosystem generally) is a drop-in
alternative that needs no new container — `spark-thriftserver` reuses the
same Spark image already built for Bronze/Silver/Gold jobs, just running
`org.apache.spark.sql.hive.thriftserver.HiveThriftServer2` instead of one of
the batch scripts.

Unlike Trino, Spark's own Hive catalog needs a real `CREATE TABLE` per Gold
table (`register_gold_tables.py`, run once automatically before the server
starts) rather than a path-only `register_table` call — Spark's Hive
metastore stores a schema at registration time instead of reading Delta's
schema live on every query like Trino's connector does. The registered name
is `gold_<table>` (e.g. `gold_sector_exposure`) to avoid colliding with
Spark's own `default` database namespace.

**Bug hit and fixed while building this**: the Thrift server started fine,
but the moment a JDBC client opened a session, every query failed with
`ClassNotFoundException: org.apache.spark.sql.delta.catalog.DeltaCatalog`.
Root cause: `spark.jars.packages` (Ivy-resolved at `spark-submit` startup)
added Delta/S3A jars to the *driver's* classpath fine for ordinary batch
jobs, but Spark Thrift Server spins up a new session per JDBC connection on
its own background thread pool, and those threads' context classloader
doesn't see jars Ivy added dynamically — only what's physically present
under `$SPARK_HOME/jars`. Fixed by downloading the exact same jars (and
their transitive dependencies Ivy was silently resolving too: `delta-storage`,
`antlr4-runtime`, `wildfly-openssl`) directly into `$SPARK_HOME/jars` at
image build time (`spark/Dockerfile`), and dropping `spark.jars.packages`
from `spark-defaults.conf` entirely. This also sped up every other job —
no more Ivy resolution on every single `spark-submit` call.

**Power BI connection**: install the **Simba Spark ODBC driver** (free), then
Get Data → More → Database → **Spark**. Server: `localhost`, Port: `8810`,
Protocol: HTTP or "Thrift"/binary depending on driver version (try binary
first — Spark Thrift Server here is the binary protocol, not HTTP), auth
type: none/user-only. Navigate to `default` schema → `gold_sector_exposure`
/ `gold_institution_risk_trends`.

Neo4j does **not** go through Trino/Power BI's path — it's a database, not a
BI client, so the plan is a separate batch loader
(`integrations/neo4j/load_graph.py`, not yet implemented) that reads Gold
`sector_exposure` directly via Spark or the Neo4j Python driver and pushes
rows in as nodes/relationships on a schedule, not live query-on-demand.

## Data portal

A supervisor/stakeholder demo needs an answer to "what data do we actually
have?" that doesn't require MinIO credentials, a Parquet/Delta reader, or
Spark — `catalog/index.html` (static HTML/CSS/JS, no build step, no external
dependencies) served by the `catalog-portal` nginx container at
http://localhost:8890. It reads `catalog/catalog.json`, a snapshot built by
`spark/jobs/catalog_builder.py`.

For every Bronze/Silver/Gold table it shows: row count, full schema, date
coverage (min/max reporting date), how many institutions it covers, when it
was last written, and — the part that actually answers "what can I do with
this" — a one-line description plus a **suggested analysis** per dataset
(e.g. `credit_exposure` → "sector concentration risk, NPL-by-sector,
institution-sector exposure graphs"). Column names and types alone don't
tell a non-technical viewer what a table is *for*; that curated text does.

The dataset list and descriptions aren't redeclared here — `catalog_builder.py`
imports the same `WIDE_DATASETS`/`PIVOTED_DATASETS`/`MASTER_DATASETS`/
`BUILDERS` dicts the actual transform jobs use, so the catalog can't drift
out of sync with what those jobs produce. `catalog.json` is written to a
local path (`/opt/catalog`, bind-mounted from the host), not into MinIO —
it's metadata *about* the lakehouse, not lakehouse data itself, and the
static file server needs it on a plain filesystem path, not behind S3A.

This is a browse/demo tool, not a real data catalog (no search indexing, no
lineage graph, no access control) — intentionally, per the same
don't-overbuild reasoning as everything else here. Amundsen/DataHub-class
tooling would be the real answer at production scale, not for a POC.

### Planned: Airflow calling Spark (Phase 5)

Not implemented yet — noted here since it shapes how the job scripts are built.
Spark runs as a single local-mode container, not a cluster with a master URL,
so `SparkSubmitOperator` (which expects a `spark://` master or a local
`spark-submit` binary on the Airflow worker) isn't a natural fit. Plan: use
`DockerOperator` so each Airflow task launches a one-off container from the
`bot_lakehouse-spark` image (same network, volumes, env vars) and runs
`spark-submit`, then exits — same pattern as `docker compose exec spark
spark-submit ...` above, just triggered by Airflow instead of a human. Requires
mounting the Docker socket into the Airflow container (standard for local
Compose setups). Both `bronze_ingestion.py` and `master_data_ingestion.py`
already take their parameters as plain CLI args for exactly this reason.

## Notes on this environment

- `trino` is pinned to `:latest` for now (unlike the Spark/MinIO images,
  there was no availability problem to work around) — pin to an exact
  version before this goes anywhere near production, same reasoning as any
  other image.
- This machine sits behind a corporate SSL-inspecting proxy (Fortinet). The Spark
  image (`spark/Dockerfile`) imports that CA into both the OS and JVM trust stores
  (`spark/certs/`) so `pip` and Maven/Ivy dependency resolution work during the
  build. If you rebuild on a different network, these certs may not be needed —
  but leaving them in the trust store is harmless.
- The `minio` service runs MinIO **AIStor** (`quay.io/minio/aistor/minio`), the
  vendor's own licensed distribution — not the community image. It requires a
  license file mounted at `/minio.license` (see `minio/minio.license`,
  gitignored, and the `MINIO_LICENSE` comment in `.env.example` for where to
  get one); root credentials (`MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD`) work the
  same as on community MinIO. Data on disk is wire-compatible with the
  community image this replaced, verified by swapping the image on the same
  `minio_data` volume with no data loss.
- `minio-init` still uses `bitnamilegacy/minio-client` (`mc`) for bucket
  creation — the client is independent of which server it talks to, so this
  wasn't changed by the AIStor switch. Docker Hub retired the official free
  `minio/mc` image in 2025; this is a frozen, unpatched snapshot, fine for a
  local POC.
- If running Bash commands against these containers from Git Bash on Windows,
  prefix with `MSYS_NO_PATHCONV=1` to stop it from mangling container paths
  like `/opt/spark-jobs/...` into Windows paths.
