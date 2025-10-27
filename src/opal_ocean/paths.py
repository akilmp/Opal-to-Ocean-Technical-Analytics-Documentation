"""Shared filesystem paths for the Opal to Ocean project."""
from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
STAGING_DIR = DATA_DIR / "staging"
MARTS_DIR = DATA_DIR / "marts"
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts"


def ensure_directories() -> None:
    """Ensure that directories required for pipeline runs exist."""
    for directory in (DATA_DIR, RAW_DIR, STAGING_DIR, MARTS_DIR, ARTIFACTS_DIR):
        directory.mkdir(parents=True, exist_ok=True)
