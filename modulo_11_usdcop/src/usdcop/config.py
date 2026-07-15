from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


@dataclass(frozen=True)
class AppPaths:
    project_root: Path
    storage_root: Path
    output_root: Path
    config_root: Path


def resolve_paths(project_root: str | Path | None = None) -> AppPaths:
    load_dotenv()
    root = Path(project_root or os.getenv("USDCOP_PROJECT_ROOT", ".")).expanduser().resolve()
    storage = root / os.getenv("USDCOP_STORAGE_ROOT", "data/store")
    output = root / os.getenv("USDCOP_OUTPUT_ROOT", "outputs")
    config = root / "config"
    storage.mkdir(parents=True, exist_ok=True)
    output.mkdir(parents=True, exist_ok=True)
    return AppPaths(root, storage, output, config)


def load_yaml(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        value = yaml.safe_load(handle) or {}
    if not isinstance(value, dict):
        raise ValueError(f"Expected a mapping in {path}")
    return value


def load_settings(project_root: str | Path | None = None) -> tuple[AppPaths, dict[str, Any], dict[str, Any]]:
    paths = resolve_paths(project_root)
    settings = load_yaml(paths.config_root / "settings.yaml")
    series = load_yaml(paths.config_root / "series.yaml")
    return paths, settings, series
