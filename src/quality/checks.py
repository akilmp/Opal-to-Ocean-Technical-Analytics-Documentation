"""Data quality checks for the mart outputs."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Tuple

import duckdb

TYPE_SYNONYMS = {
    'BIGINT': {'BIGINT', 'HUGEINT'},
    'DOUBLE': {'DOUBLE', 'FLOAT', 'DECIMAL', 'NUMERIC'},
    'VARCHAR': {'VARCHAR'},
    'BOOLEAN': {'BOOLEAN'},
    'DATE': {'DATE'}
}

EXPECTED_SCHEMAS: Dict[str, Dict[str, str]] = {
    "stg_commute_day": {
        "date": "DATE",
        "primary_mode": "VARCHAR",
        "commute_minutes": "DOUBLE",
        "opal_cost": "DOUBLE",
        "late_trip_count": "BIGINT",
        "total_trip_count": "BIGINT",
        "mean_delay_s": "DOUBLE",
    },
    "stg_env_day": {
        "date": "DATE",
        "primary_station_id": "VARCHAR",
        "pm25_mean": "DOUBLE",
        "pm10_mean": "DOUBLE",
        "rain_24h": "DOUBLE",
        "temp_mean_c": "DOUBLE",
        "beach_status_primary": "VARCHAR",
        "enterococci_mean": "DOUBLE",
        "beach_site_id": "VARCHAR",
    },
    "dim_commute_mode": {
        "primary_mode": "VARCHAR",
        "day_count": "BIGINT",
        "avg_minutes": "DOUBLE",
        "avg_delay_s": "DOUBLE",
        "late_trip_rate": "DOUBLE",
        "commute_band": "VARCHAR",
    },
    "dim_environment_metric": {
        "date": "DATE",
        "primary_station_id": "VARCHAR",
        "pm25_mean": "DOUBLE",
        "pm10_mean": "DOUBLE",
        "rain_24h": "DOUBLE",
        "temp_mean_c": "DOUBLE",
        "beach_status_primary": "VARCHAR",
        "enterococci_mean": "DOUBLE",
        "beach_site_id": "VARCHAR",
        "pm25_category": "VARCHAR",
        "rainfall_category": "VARCHAR",
        "is_swimmable": "BOOLEAN",
    },
}

RANGE_CHECKS: Dict[str, Dict[str, Tuple[float | None, float | None]]] = {
    "stg_commute_day": {
        "commute_minutes": (0, 24 * 60),
        "opal_cost": (0, None),
        "late_trip_count": (0, None),
        "total_trip_count": (0, None),
        "mean_delay_s": (-6 * 3600, 6 * 3600),
    },
    "stg_env_day": {
        "pm25_mean": (0, 500),
        "pm10_mean": (0, 800),
        "rain_24h": (0, 1000),
        "temp_mean_c": (-10, 50),
        "enterococci_mean": (0, None),
    },
}

FRESHNESS_CHECKS: Dict[str, Dict[str, Any]] = {
    "stg_commute_day": {"column": "date", "max_age_days": 7},
    "stg_env_day": {"column": "date", "max_age_days": 7},
    "dim_environment_metric": {"column": "date", "max_age_days": 7},
}


def _describe_table(con: duckdb.DuckDBPyConnection, table: str) -> Dict[str, str]:
    result = con.execute(f"DESCRIBE {table}").fetchall()
    return {row[0]: row[1] for row in result}


def _table_has_rows(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    return bool(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _types_match(expected: str, actual: str) -> bool:
    expected_upper = expected.upper()
    actual_upper = actual.upper()
    if expected_upper == actual_upper:
        return True
    allowed = TYPE_SYNONYMS.get(expected_upper)
    if allowed and actual_upper in allowed:
        return True
    return False


def _check_schema(con: duckdb.DuckDBPyConnection, table: str, expected: Dict[str, str]) -> Dict[str, Any]:
    actual = _describe_table(con, table)
    missing = [col for col in expected if col not in actual]
    mismatched = {}
    for col, expected_type in expected.items():
        actual_type = actual.get(col)
        if actual_type is None:
            continue
        if not _types_match(expected_type, actual_type):
            mismatched[col] = {"expected": expected_type, "actual": actual_type}

    status = "pass" if not missing and not mismatched else "fail"
    return {
        "status": status,
        "missing_columns": missing,
        "mismatched_types": mismatched,
    }


def _check_ranges(
    con: duckdb.DuckDBPyConnection,
    table: str,
    rules: Dict[str, Tuple[float | None, float | None]],
) -> Dict[str, Any]:
    if not _table_has_rows(con, table):
        return {"status": "skipped", "reason": "table is empty"}

    failures = {}
    for column, (lower, upper) in rules.items():
        query = f"SELECT MIN({column}), MAX({column}) FROM {table}"
        min_val, max_val = con.execute(query).fetchone()
        out_of_bounds = False
        if lower is not None and min_val is not None and min_val < lower:
            out_of_bounds = True
        if upper is not None and max_val is not None and max_val > upper:
            out_of_bounds = True
        if out_of_bounds:
            failures[column] = {
                "min": min_val,
                "max": max_val,
                "expected_min": lower,
                "expected_max": upper,
            }
    status = "pass" if not failures else "fail"
    return {"status": status, "violations": failures}


def _check_freshness(
    con: duckdb.DuckDBPyConnection,
    table: str,
    column: str,
    max_age_days: int,
) -> Dict[str, Any]:
    if not _table_has_rows(con, table):
        return {"status": "skipped", "reason": "table is empty"}
    query = f"SELECT MAX({column}) FROM {table}"
    max_date = con.execute(query).fetchone()[0]
    if max_date is None:
        return {"status": "fail", "reason": "max date is NULL"}
    today = datetime.now(timezone.utc).date()
    age_days = (today - max_date).days
    status = "pass" if age_days <= max_age_days else "fail"
    return {
        "status": status,
        "max_value": str(max_date),
        "age_days": age_days,
        "max_age_days": max_age_days,
    }


def run_quality_checks(
    con: duckdb.DuckDBPyConnection,
    output_dir: Path,
) -> Dict[str, Any]:
    logging.info("Running data quality checks")
    report: Dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tables": {},
    }
    overall_success = True

    for table, expected_schema in EXPECTED_SCHEMAS.items():
        table_report: Dict[str, Any] = {}
        table_report["schema"] = _check_schema(con, table, expected_schema)
        if table_report["schema"]["status"] == "fail":
            overall_success = False

        if table in RANGE_CHECKS:
            range_result = _check_ranges(con, table, RANGE_CHECKS[table])
            table_report["ranges"] = range_result
            if range_result["status"] == "fail":
                overall_success = False

        if table in FRESHNESS_CHECKS:
            freshness_cfg = FRESHNESS_CHECKS[table]
            freshness_result = _check_freshness(
                con,
                table,
                freshness_cfg["column"],
                freshness_cfg["max_age_days"],
            )
            table_report["freshness"] = freshness_result
            if freshness_result["status"] == "fail":
                overall_success = False

        report["tables"][table] = table_report

    report["success"] = overall_success
    return report
