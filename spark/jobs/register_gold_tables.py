"""One-time (idempotent) registration of Gold Delta tables into the local Hive
metastore that Spark Thrift Server uses, so JDBC/ODBC clients (Power BI via
the free Simba Spark ODBC driver) can query them by name (`gold_sector_exposure`)
instead of by S3 path -- the Spark-Thrift-Server equivalent of Trino's
`register_table` procedure used for the delta.gold schema there.

Must run with the SAME working directory as the thrift server process (both
default to ./metastore_db and ./spark-warehouse relative to CWD when Hive
support is enabled with no explicit config) -- docker-compose pins
`working_dir: /opt/spark-thriftserver` on the spark-thriftserver service for
exactly this reason, and this script is run once before the server starts,
in sequence, inside the same container.

Usage: spark-submit /opt/spark-jobs/register_gold_tables.py
"""

import logging
import os

from pyspark.sql import SparkSession

from gold_transform import BUILDERS as GOLD_TABLES
from spark_session import lakehouse_path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("register_gold_tables")


def main():
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    minio_user = os.environ["MINIO_ROOT_USER"]
    minio_password = os.environ["MINIO_ROOT_PASSWORD"]

    spark = (
        SparkSession.builder.appName("register-gold-tables")
        .enableHiveSupport()
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_user)
        .config("spark.hadoop.fs.s3a.secret.key", minio_password)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )

    for table in sorted(GOLD_TABLES):
        path = lakehouse_path("gold", "bsis", table)
        logger.info("Registering gold_%s -> %s", table, path)
        spark.sql(f"CREATE TABLE IF NOT EXISTS gold_{table} USING DELTA LOCATION '{path}'")

    spark.sql("SHOW TABLES").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
