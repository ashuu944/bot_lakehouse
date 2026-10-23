"""Loads per-directorate/source-system dataset definitions from
spark/config/sources/<directorate>/<source_system>.yaml.

This is what replaces hardcoded directorate/source-system literals (e.g. the
old `lakehouse_path("bronze", "DFSS", "BSIS", dataset)`) and per-dataset
Python dicts in bronze_ingestion.py/silver_transform.py: onboarding a new
directorate or source system means adding a YAML file here, not a new code
path. bronze_ingestion.py and silver_transform.py both read the same index
this module builds, so there's exactly one place a dataset's directorate,
source system, and transformation spec are declared.
"""

from __future__ import annotations

import glob
import os

import yaml

CONFIG_DIR = os.environ.get("SPARK_CONFIG_DIR", "/opt/spark-config")

_REQUIRED_SOURCE_KEYS = ("directorate", "source_system", "datasets")


def _config_paths() -> list[str]:
    return sorted(glob.glob(os.path.join(CONFIG_DIR, "sources", "**", "*.yaml"), recursive=True))


def load_source_configs() -> list[dict]:
    paths = _config_paths()
    if not paths:
        raise RuntimeError(f"No source configs found under {CONFIG_DIR}/sources/**/*.yaml")

    configs = []
    for path in paths:
        with open(path) as f:
            cfg = yaml.safe_load(f)
        missing = [k for k in _REQUIRED_SOURCE_KEYS if k not in cfg]
        if missing:
            raise ValueError(f"{path}: missing required key(s) {missing}")
        configs.append(cfg)
    return configs


def build_dataset_index(configs: list[dict] | None = None) -> dict[str, dict]:
    """dataset name -> its config entry, merged with its source's directorate/
    source_system/extractor_module. Dataset names must be globally unique
    across every configured source (not just within one file) -- Bronze/
    Silver paths are built from the dataset name alone, so a collision would
    silently mean two different datasets writing to the same place.
    """
    configs = configs if configs is not None else load_source_configs()
    index: dict[str, dict] = {}
    for cfg in configs:
        for ds in cfg["datasets"]:
            name = ds["dataset"]
            if name in index:
                prev = index[name]
                raise ValueError(
                    f"Dataset {name!r} is declared twice: "
                    f"{prev['directorate']}/{prev['source_system']} and {cfg['directorate']}/{cfg['source_system']}"
                )
            index[name] = {
                **ds,
                "directorate": cfg["directorate"],
                "source_system": cfg["source_system"],
                "extractor_module": cfg.get("extractor_module"),
            }
    return index
