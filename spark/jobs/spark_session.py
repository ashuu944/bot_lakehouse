"""Shared SparkSession factory: Delta Lake tables stored in MinIO (S3A).

Delta/S3A jars and Delta SQL extensions are configured in
spark/conf/spark-defaults.conf (baked into the image), since
spark.jars.packages must be known before spark-submit launches the JVM
and has no effect if only set here in the driver script.
"""

import os

from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    minio_user = os.environ["MINIO_ROOT_USER"]
    minio_password = os.environ["MINIO_ROOT_PASSWORD"]

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_user)
        .config("spark.hadoop.fs.s3a.secret.key", minio_password)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )
    return builder.getOrCreate()


def lakehouse_path(layer: str, *parts: str) -> str:
    """Build an s3a:// path under the lakehouse bucket, e.g. lakehouse_path('bronze', 'bsis', 'accounts')."""
    bucket = os.environ.get("MINIO_BUCKET", "lakehouse")
    segments = "/".join(parts)
    return f"s3a://{bucket}/{layer}/{segments}" if segments else f"s3a://{bucket}/{layer}"
