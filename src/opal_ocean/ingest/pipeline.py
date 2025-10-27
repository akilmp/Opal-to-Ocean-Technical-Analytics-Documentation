"""Live ingestion orchestration for the Opal to Ocean project."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional

import pandas as pd

from ingest.air_quality import AirQualityNSWClient
from ingest.abs_sa2 import ABSSA2ContextClient
from ingest.beachwatch import BeachwatchClient
from ingest.pedestrian import CityOfSydneyPedestrianClient
from ingest.tfnws import TfNSWGTFSRealtimeClient, TfNSWGTFSStaticClient
from ingest.utils import get_env_list, persist_dataframe

from ..logging import get_logger, log_event
from ..paths import RAW_DIR

LOGGER = get_logger("ingest.pipeline")

DEFAULT_STATIC_FEEDS = ["sydneytrains"]
DEFAULT_REALTIME_FEEDS = ["buses"]


@dataclass
class PartitionResult:
    """Metadata describing a single persisted partition."""

    path: Path
    rows: int
    metadata: Dict[str, object] = field(default_factory=dict)


@dataclass
class DatasetResult:
    """Aggregated ingestion details for a dataset."""

    name: str
    partitions: List[PartitionResult] = field(default_factory=list)
    metadata: Dict[str, object] = field(default_factory=dict)

    @property
    def rows(self) -> int:
        return sum(part.rows for part in self.partitions)

    def as_dict(self) -> Dict[str, object]:
        return {
            "dataset": self.name,
            "rows": self.rows,
            "metadata": self.metadata,
            "partitions": [
                {
                    "path": str(part.path),
                    "rows": part.rows,
                    "metadata": part.metadata,
                }
                for part in self.partitions
            ],
        }


def _persist_partition(
    df: pd.DataFrame,
    *,
    source: str,
    ingest_date: date,
    metadata: Mapping[str, object],
) -> PartitionResult:
    destination = persist_dataframe(
        df,
        source=source,
        ingest_date=ingest_date,
        base_dir=RAW_DIR,
    )
    rows = len(df)
    log_event(
        LOGGER,
        "dataset_partition_written",
        dataset=source,
        path=str(destination),
        rows=rows,
        **{k: v for k, v in metadata.items()},
    )
    serializable_metadata: Dict[str, object] = {
        key: _coerce_metadata_value(value) for key, value in metadata.items()
    }
    return PartitionResult(path=destination, rows=rows, metadata=serializable_metadata)


def _coerce_metadata_value(value: object) -> object:
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return [_coerce_metadata_value(item) for item in value]
    return value


def _ingest_tfnws_static(ingest_date: date) -> DatasetResult:
    feed_codes = get_env_list("TFNSW_STATIC_FEED_CODES", ",".join(DEFAULT_STATIC_FEEDS))
    client = TfNSWGTFSStaticClient()
    result = DatasetResult(
        name="tfnws_static",
        metadata={"feed_codes": feed_codes},
    )
    for feed_code in feed_codes:
        df = client.fetch_feed(feed_code)
        retrieved_at: Optional[datetime] = None
        if not df.empty and "retrieved_at" in df:
            retrieved_at = pd.to_datetime(df.loc[0, "retrieved_at"]).to_pydatetime()
        partition = _persist_partition(
            df,
            source="tfnws_static",
            ingest_date=ingest_date,
            metadata={"feed_code": feed_code, "retrieved_at": retrieved_at},
        )
        result.partitions.append(partition)
    return result


def _ingest_tfnws_realtime(ingest_date: date) -> DatasetResult:
    feed_codes = get_env_list("TFNSW_REALTIME_FEED_CODES", ",".join(DEFAULT_REALTIME_FEEDS))
    client = TfNSWGTFSRealtimeClient()
    result = DatasetResult(
        name="tfnws_realtime",
        metadata={"feed_codes": feed_codes},
    )
    for feed_code in feed_codes:
        df = client.fetch_feed(feed_code)
        retrieved_at: Optional[datetime] = None
        if not df.empty and "retrieved_at" in df:
            retrieved_at = pd.to_datetime(df.loc[0, "retrieved_at"]).to_pydatetime()
        partition = _persist_partition(
            df,
            source="tfnws_realtime",
            ingest_date=ingest_date,
            metadata={"feed_code": feed_code, "retrieved_at": retrieved_at},
        )
        result.partitions.append(partition)
    return result


def _ingest_beachwatch(ingest_date: date, *, limit: int = 1000) -> DatasetResult:
    client = BeachwatchClient()
    dataset_id = client._dataset_id()  # type: ignore[attr-defined]
    df = client.fetch_measurements(limit=limit)
    partition = _persist_partition(
        df,
        source="beachwatch",
        ingest_date=ingest_date,
        metadata={"dataset_id": dataset_id, "limit": limit},
    )
    return DatasetResult(
        name="beachwatch",
        metadata={"dataset_id": dataset_id, "limit": limit},
        partitions=[partition],
    )


def _ingest_air_quality(ingest_date: date, *, limit: int = 1000) -> DatasetResult:
    client = AirQualityNSWClient()
    dataset_id = client._dataset_id()  # type: ignore[attr-defined]
    df = client.fetch_measurements(limit=limit)
    partition = _persist_partition(
        df,
        source="air_quality_nsw",
        ingest_date=ingest_date,
        metadata={"dataset_id": dataset_id, "limit": limit},
    )
    return DatasetResult(
        name="air_quality_nsw",
        metadata={"dataset_id": dataset_id, "limit": limit},
        partitions=[partition],
    )


def _ingest_pedestrian(ingest_date: date, *, limit: int = 1000) -> DatasetResult:
    client = CityOfSydneyPedestrianClient()
    dataset_id = client._dataset_id()  # type: ignore[attr-defined]
    df = client.fetch_counts(limit=limit)
    partition = _persist_partition(
        df,
        source="city_of_sydney_pedestrian",
        ingest_date=ingest_date,
        metadata={"dataset_id": dataset_id, "limit": limit},
    )
    return DatasetResult(
        name="city_of_sydney_pedestrian",
        metadata={"dataset_id": dataset_id, "limit": limit},
        partitions=[partition],
    )


def _ingest_abs_sa2(ingest_date: date, *, limit: int = 1000) -> DatasetResult:
    client = ABSSA2ContextClient()
    dataset_id = client._dataset_id()  # type: ignore[attr-defined]
    df = client.fetch_context(limit=limit)
    partition = _persist_partition(
        df,
        source="abs_sa2_context",
        ingest_date=ingest_date,
        metadata={"dataset_id": dataset_id, "limit": limit},
    )
    return DatasetResult(
        name="abs_sa2_context",
        metadata={"dataset_id": dataset_id, "limit": limit},
        partitions=[partition],
    )


def _build_manifest(run_id: str, outputs: Iterable[DatasetResult]) -> Dict[str, object]:
    return {
        "run_id": run_id,
        "pipeline": "ingest",
        "generated_at": datetime.now(UTC).isoformat(),
        "outputs": [result.as_dict() for result in outputs],
    }


def _quality_checks(run_id: str, outputs: Iterable[DatasetResult]) -> Dict[str, object]:
    checks = []
    for result in outputs:
        check: Dict[str, object] = {
            "dataset": result.name,
            "rows": result.rows,
            "status": "pass" if result.rows > 0 else "fail",
            "partitions": [
                {
                    "path": str(part.path),
                    "rows": part.rows,
                    "metadata": part.metadata,
                }
                for part in result.partitions
            ],
        }
        if result.metadata:
            check["metadata"] = result.metadata
        checks.append(check)
    return {
        "run_id": run_id,
        "pipeline": "ingest",
        "generated_at": datetime.now(UTC).isoformat(),
        "checks": checks,
    }


def run_ingest(run_id: str, run_dir: Path) -> tuple[Path, Path]:
    """Execute the live ingestion workflow and emit manifest/quality reports."""

    ingest_date = datetime.now(UTC).date()
    datasets: List[DatasetResult] = []

    for dataset in (
        _ingest_tfnws_static(ingest_date),
        _ingest_tfnws_realtime(ingest_date),
        _ingest_beachwatch(ingest_date),
        _ingest_air_quality(ingest_date),
        _ingest_pedestrian(ingest_date),
        _ingest_abs_sa2(ingest_date),
    ):
        datasets.append(dataset)
        log_event(
            LOGGER,
            "dataset_ingested",
            dataset=dataset.name,
            rows=dataset.rows,
            partitions=len(dataset.partitions),
        )

    manifest = _build_manifest(run_id, datasets)
    quality = _quality_checks(run_id, datasets)

    manifest_path = run_dir / "run_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))
    quality_path = run_dir / "quality_report.json"
    quality_path.write_text(json.dumps(quality, indent=2, sort_keys=True))

    log_event(
        LOGGER,
        "ingest_run_completed",
        run_id=run_id,
        manifest=str(manifest_path),
        quality_report=str(quality_path),
    )

    return manifest_path, quality_path
