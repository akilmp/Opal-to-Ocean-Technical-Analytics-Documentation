"""Ingestion clients and batch jobs for NSW transport and environment datasets."""

from .tfnws import TfNSWGTFSStaticClient, TfNSWGTFSRealtimeClient
from .beachwatch import BeachwatchClient
from .air_quality import AirQualityNSWClient
from .pedestrian import CityOfSydneyPedestrianClient
from .abs_sa2 import ABSSA2ContextClient
from .jobs import (
    run_tfnws_static_job,
    run_tfnws_realtime_job,
    run_beachwatch_job,
    run_air_quality_job,
    run_pedestrian_counts_job,
    run_abs_sa2_context_job,
)

__all__ = [
    "TfNSWGTFSStaticClient",
    "TfNSWGTFSRealtimeClient",
    "BeachwatchClient",
    "AirQualityNSWClient",
    "CityOfSydneyPedestrianClient",
    "ABSSA2ContextClient",
    "run_tfnws_static_job",
    "run_tfnws_realtime_job",
    "run_beachwatch_job",
    "run_air_quality_job",
    "run_pedestrian_counts_job",
    "run_abs_sa2_context_job",
]
