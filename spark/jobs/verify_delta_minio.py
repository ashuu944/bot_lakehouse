"""Phase 1 check: prove Spark can write and read a Delta table stored in MinIO.

Usage (run from the project root, containers already up via
`docker compose up -d minio spark`):

    docker compose exec spark spark-submit /opt/spark-jobs/verify_delta_minio.py

Expect "PHASE 1 OK: wrote and read back 3 rows via Delta on MinIO." at the end.
"""

from spark_session import get_spark, lakehouse_path


def main():
    spark = get_spark("verify-delta-minio")
    table_path = lakehouse_path("bronze", "_verify", "phase1_check")

    data = [(1, "alpha"), (2, "beta"), (3, "gamma")]
    df = spark.createDataFrame(data, ["id", "name"])

    print(f"Writing Delta table to {table_path}")
    df.write.format("delta").mode("overwrite").save(table_path)

    print(f"Reading Delta table back from {table_path}")
    result = spark.read.format("delta").load(table_path)
    result.show()

    written_count = df.count()
    read_count = result.count()
    assert read_count == written_count, (
        f"Row count mismatch: wrote {written_count}, read {read_count}"
    )

    print(f"PHASE 1 OK: wrote and read back {read_count} rows via Delta on MinIO.")
    spark.stop()


if __name__ == "__main__":
    main()
