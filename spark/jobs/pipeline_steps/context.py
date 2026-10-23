"""Per-run state shared across steps: the SparkSession and a cache of Silver
reference tables (institutions, sectors, ...) that many datasets in the same
run join against -- without this, each dataset processed in one spark-submit
invocation would re-read the same small dimension table from MinIO again.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession

from spark_session import lakehouse_path


@dataclass
class PipelineContext:
    spark: SparkSession
    _cache: dict[str, DataFrame] = field(default_factory=dict)

    def read_silver(self, path: str) -> DataFrame:
        """Load a Silver Delta table by its path under silver/, e.g. 'master/institutions'."""
        if path not in self._cache:
            self._cache[path] = self.spark.read.format("delta").load(lakehouse_path("silver", *path.split("/")))
        return self._cache[path]
