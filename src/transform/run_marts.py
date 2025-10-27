from __future__ import annotations

"""Runner that materialises DuckDB marts and exports them to data/marts."""

import json
import logging
from pathlib import Path
from typing import Dict, List, Tuple, TypedDict

import duckdb

from src.quality.checks import run_quality_checks


class RawSourceConfig(TypedDict):
    path: Path
    glob: str
    schema: str


ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = Path(__file__).resolve().parent / "sql"
OUTPUT_DIR = ROOT / "data" / "marts"
DUCKDB_PATH = OUTPUT_DIR / "marts.duckdb"

RAW_SOURCES: Dict[str, RawSourceConfig] = {
    "commute_trips": {
        "path": ROOT / "data" / "raw" / "commute",
        "glob": "*.parquet",
        "schema": "trip_id VARCHAR, trip_start_ts TIMESTAMP, primary_mode VARCHAR, trip_minutes DOUBLE, fare_amount DOUBLE, delay_seconds DOUBLE",
    },
    "air_quality": {
        "path": ROOT / "data" / "raw" / "air_quality",
        "glob": "*.parquet",
        "schema": "station_id VARCHAR, observation_ts TIMESTAMP, pm25 DOUBLE, pm10 DOUBLE",
    },
    "weather": {
        "path": ROOT / "data" / "raw" / "weather",
        "glob": "*.parquet",
        "schema": "observation_ts TIMESTAMP, rain_mm DOUBLE, temperature_c DOUBLE",
    },
    "beachwatch": {
        "path": ROOT / "data" / "raw" / "beachwatch",
        "glob": "*.parquet",
        "schema": "observation_ts TIMESTAMP, site_id VARCHAR, status VARCHAR, enterococci DOUBLE",
    },
}

SQL_MODELS: List[Tuple[str, Path]] = [
    ("stg_commute_day", SQL_DIR / "stg_commute_day.sql"),
    ("stg_env_day", SQL_DIR / "stg_env_day.sql"),
    ("dim_commute_mode", SQL_DIR / "dim_commute_mode.sql"),
    ("dim_environment_metric", SQL_DIR / "dim_environment_metric.sql"),
]


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def _register_raw_sources(con: duckdb.DuckDBPyConnection) -> None:
    for name, cfg in RAW_SOURCES.items():
        table_name = f"raw_{name}"
        base_path = cfg["path"]
        glob_pattern = cfg["glob"]
        files = sorted(base_path.glob(glob_pattern)) if base_path.exists() else []
        if files:
            pattern = str(base_path / glob_pattern)
            logging.info("Registering %s from %s", table_name, pattern)
            con.execute(
                f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{pattern}')"
            )
        else:
            logging.warning(
                "No files found for %s. Creating empty table with expected schema.",
                table_name,
            )
            con.execute(
                f"CREATE OR REPLACE TABLE {table_name} ({cfg['schema']})"
            )


def _load_sql(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"SQL model not found: {path}")
    return path.read_text()


def _run_model(con: duckdb.DuckDBPyConnection, name: str, sql_path: Path) -> None:
    logging.info("Running model %s", name)
    sql = _load_sql(sql_path)
    con.execute(sql)


def _export_table(con: duckdb.DuckDBPyConnection, table: str, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / f"{table}.parquet"
    logging.info("Exporting %s to %s", table, target)
    con.execute(
        f"COPY (SELECT * FROM {table} ORDER BY 1) TO '{target}' (FORMAT PARQUET)"
    )
    return target


def run() -> None:
    _configure_logging()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logging.info("Materialising marts at %s", OUTPUT_DIR)

    con = duckdb.connect(str(DUCKDB_PATH))
    try:
        _register_raw_sources(con)
        for name, sql_path in SQL_MODELS:
            _run_model(con, name, sql_path)
            _export_table(con, name, OUTPUT_DIR)

        quality_report = run_quality_checks(con, OUTPUT_DIR)
        (OUTPUT_DIR / "quality_report.json").write_text(
            json.dumps(quality_report, indent=2, default=str)
        )

        if not quality_report.get("success", False):
            raise RuntimeError("Data quality checks failed. See quality_report.json for details.")

        logging.info("Marts built successfully.")
    finally:
        con.close()


if __name__ == "__main__":
    run()
