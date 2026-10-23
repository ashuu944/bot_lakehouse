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
- **Custom data portal** (`catalog/`, nginx) — a lightweight static page
  listing every table with its schema, row counts, date/institution
  coverage, and suggested analyses, for demos without needing a Parquet
  reader.
- **Power BI / Neo4j** — downstream consumers: Power BI reads Gold via
  Trino/Spark Thrift Server; Neo4j is planned to get a batch graph load from
  Gold's edge-shaped tables (e.g. institution ↔ sector exposure).

## Access (local)

| Service | URL |
|---|---|
| MinIO console | http://localhost:9001 |
| Trino | http://localhost:8880 |
| Spark Thrift Server | JDBC/ODBC on localhost:8810 |
| Airflow | http://localhost:8895 |
| OpenMetadata | http://localhost:8585 |
| Data portal | http://localhost:8890 |

## Setup

1. Copy `.env.example` to `.env` and fill in credentials (MinIO, DB, etc.).
2. Build and start everything:
   ```
   docker compose up -d
   ```
3. Airflow DAGs (`daily_bsis_pipeline_dag`, `monthly_bsis_pipeline_dag`,
   `backfill_bsis_pipeline_dag`, `openmetadata_ingestion_dag`) handle running
   the Spark jobs and keeping OpenMetadata's catalog in sync — trigger them
   from the Airflow UI.
