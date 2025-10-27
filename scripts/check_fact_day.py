"""Quick health check for the fact_day mart output."""
from __future__ import annotations

import sys
from pathlib import Path

import pyarrow.parquet as pq

from opal_ocean.transform.pipeline import FACT_DAY_COLUMNS


DEFAULT_PATH = Path("data/marts/fact_day.parquet")


def main(path: str | None = None) -> int:
    """Validate that fact_day exists and matches the expected schema."""
    target = Path(path) if path else DEFAULT_PATH
    if not target.exists():
        sys.stderr.write(f"Missing fact table: {target}\n")
        return 1

    table = pq.read_table(target)
    columns = list(table.schema.names)

    if columns != FACT_DAY_COLUMNS:
        missing = [c for c in FACT_DAY_COLUMNS if c not in columns]
        extra = [c for c in columns if c not in FACT_DAY_COLUMNS]
        sys.stderr.write(
            "Schema mismatch for fact_day.\n"
            f"Expected columns: {FACT_DAY_COLUMNS}\n"
            f"Found columns:    {columns}\n"
        )
        if missing:
            sys.stderr.write(f"Missing columns: {missing}\n")
        if extra:
            sys.stderr.write(f"Unexpected columns: {extra}\n")
        if not missing and not extra:
            sys.stderr.write(
                "Columns match but order differs. Ensure the parquet was built with the current transforms.\n"
            )
        return 1

    if table.num_rows == 0:
        sys.stderr.write("fact_day.parquet contains zero rows.\n")
        return 1

    sys.stdout.write(
        f"fact_day health check passed — {target} has {table.num_rows} rows and expected schema.\n"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
