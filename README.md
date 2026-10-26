# bot_lakehouse

On-premise lakehouse proof of concept for banking supervisory data (BSIS, via
the `bsis_extractor` library), built as a medallion architecture — Bronze
(raw) → Silver (cleaned/joined) → Gold (business marts) — on open-source
infra, no cloud dependency.

## Tech stack

- **MinIO (AIStor)** — S3-compatible object storage; the lakehouse itself
  (`bronze/`, `silver/`, `gold/` prefixes in the `lakehouse` bucket).
- **Spark + Delta Lake** — all ingestion, transformation, and upsert (MERGE)
  logic, running as jobs in a single Spark container.
- **Trino** — SQL query engine in front of Gold Delta tables for BI tools
  (Power BI) without copying data out of MinIO.
- **Spark Thrift Server** — backup JDBC/ODBC path for Power BI, as an
  alternative to Trino.
- **Airflow** — orchestrates the daily/monthly/backfill Spark pipelines
  (`airflow/dags/`).
- **OpenMetadata** — data catalog and lineage: indexes the Bronze/Silver/Gold
  tables, schemas, and pipeline runs so the data is discoverable and
  searchable (`om-server`, `om-postgres`, `om-elasticsearch`,
  `openmetadata-ingestion`).
- **Power BI / Neo4j** — downstream consumers: Power BI reads Gold via
  Trino/Spark Thrift Server; Neo4j is planned to get a batch graph load from
  Gold's edge-shaped tables (e.g. institution ↔ sector exposure).

## Access

Every service is published straight to the Docker host on these ports (no
reverse proxy in front). On your own laptop that's `localhost`; on a shared
test server it's that server's IP or hostname — same ports either way.

| Service | URL |
|---|---|
| MinIO console | http://\<HOST\>:9001 |
| MinIO S3 API | http://\<HOST\>:9000 |
| Trino | http://\<HOST\>:8880 |
| Spark Thrift Server | JDBC/ODBC on \<HOST\>:8810 |
| Airflow | http://\<HOST\>:8895 |
| OpenMetadata | http://\<HOST\>:8585 |

`<HOST>` = `localhost` for local dev, or the test server's IP/hostname for a
remote deployment.

## Setup

1. Copy `.env.example` to `.env` and fill in credentials (MinIO, DB, etc.).
   - **`HOST_PROJECT_DIR` must be set to this project's absolute path on
     *this* machine** — it's used by Airflow's DockerOperator to talk back
     to the host's Docker daemon, so copying this value from another
     machine's `.env` will break the Spark/OpenMetadata-ingestion DAGs.
   - `OM_INGESTION_BOT_TOKEN` can't be filled in until after first boot (see
     step 5) — a fresh deployment always starts with this blank/stale.
2. **On Linux (including a fresh clone on a remote server), fix `airflow/logs`
   permissions before first boot:**
   ```
   mkdir -p airflow/logs
   sudo chown -R 50000:0 airflow/logs
   sudo chmod -R 775 airflow/logs
   ```
   Airflow's containers run as UID `50000`, group `0` (`AIRFLOW_UID` in
   `docker-compose.yml`). If this directory doesn't already have group-write
   permission for group `0` -- the default for a directory Docker
   auto-creates for a missing bind mount is `root:root` mode `755`, which
   isn't enough -- the DAG file processor crash-loops on a `PermissionError`
   writing its own log file, and **no DAGs will show up in the Airflow UI at
   all** (not even as an import error -- the component that would report one
   never gets that far). Not needed on Docker Desktop (Windows/Mac), which
   doesn't enforce host-side UID ownership on bind mounts the same way.
3. Build and start everything:
   ```
   docker compose up -d
   ```
4. On a remote test server, open these ports on the host firewall /
   security group so teammates can reach the stack: `9000`, `9001`, `8880`,
   `8810`, `8895`, `8585`, `8586`. (`om-elasticsearch`'s `9200` is
   intentionally **not** published — it has no auth and nothing outside the
   docker network needs to reach it directly.)
5. Log into OpenMetadata (http://\<HOST\>:8585, `admin@open-metadata.org` /
   `admin`), go to Settings → Bots → ingestion-bot, generate a token, and
   put it in `.env` as `OM_INGESTION_BOT_TOKEN`, then
   `docker compose up -d airflow-webserver airflow-scheduler` to pick it up.
6. Airflow DAGs (`daily_bsis_pipeline_dag`, `monthly_bsis_pipeline_dag`,
   `backfill_bsis_pipeline_dag`, `master_data_pipeline_dag`,
   `openmetadata_ingestion_dag`) handle running the Spark jobs and keeping
   OpenMetadata's catalog in sync — trigger them from the Airflow UI.
   `dfss_bsis_master_data` (master/reference data) and `dfss_bsis_backfill`
   are both on-demand only (`schedule=None`) — trigger them by hand whenever
   needed, they don't run on their own.

Note: the `bsis_extractor` DB connection (`DB_DSN`) assumes network access to
the BSIS Oracle DB that only exists on the original dev machine/network. If
the test server can't reach it, everything above still comes up and is
reachable — only the DAGs that extract fresh BSIS data will fail, which is
expected until that DB access is set up on the new host. (`MASTER_DATA_PATH`
no longer has this problem -- the master-data CSVs now live inside this repo
at `spark/master_data/DFSS/BSIS/`, so they travel with the clone.)
