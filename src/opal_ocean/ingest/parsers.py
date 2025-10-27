"""Parsing utilities for raw ingestion feeds."""
from __future__ import annotations

from typing import Final

STATUS_MAP: Final[dict[str, str]] = {
    "very good": "Very good",
    "good": "Good",
    "poor": "Poor",
    "closed": "Closed",
}


class UnknownStatusError(ValueError):
    """Raised when a Beachwatch status cannot be normalized."""


def parse_beachwatch_status(value: str) -> str:
    """Normalise Beachwatch beach status strings to canonical casing.

    Args:
        value: Raw status string as received from the API feed.

    Returns:
        Canonical representation with capitalisation matching published categories.

    Raises:
        UnknownStatusError: If the value does not map to a known status.
    """

    if value is None:
        raise UnknownStatusError("Status cannot be None")

    cleaned = value.strip().lower()
    if cleaned not in STATUS_MAP:
        raise UnknownStatusError(f"Unknown Beachwatch status: {value!r}")
    return STATUS_MAP[cleaned]
