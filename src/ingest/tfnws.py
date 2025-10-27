"""Transport for NSW GTFS clients."""

from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd

from .base import APIConfig, BaseAPIClient

logger = logging.getLogger(__name__)


DEFAULT_STATIC_BASE_URL = "https://api.transport.nsw.gov.au/v1/gtfs/schedule"
DEFAULT_REALTIME_BASE_URL = "https://api.transport.nsw.gov.au/v1/gtfs/vehiclepos"


class TfNSWGTFSStaticClient(BaseAPIClient):
    """Client for Transport for NSW GTFS static feeds."""

    def __init__(
        self,
        *,
        base_url: str = DEFAULT_STATIC_BASE_URL,
        api_key_env: str = "TFNSW_API_KEY",
    ) -> None:
        super().__init__(
            APIConfig(
                base_url=base_url,
                api_key_env=api_key_env,
                default_headers={"Accept": "application/zip"},
            )
        )

    def fetch_feed(self, feed_code: str) -> pd.DataFrame:
        """Fetch a static GTFS feed as a binary payload."""

        logger.info("Requesting TfNSW static feed: %s", feed_code)
        content = self.get_bytes(feed_code)
        now = datetime.utcnow()
        df = pd.DataFrame(
            [
                {
                    "feed_code": feed_code,
                    "retrieved_at": now,
                    "gtfs_zip": content,
                }
            ]
        )
        return df


class TfNSWGTFSRealtimeClient(BaseAPIClient):
    """Client for Transport for NSW GTFS-realtime feeds."""

    def __init__(
        self,
        *,
        base_url: str = DEFAULT_REALTIME_BASE_URL,
        api_key_env: str = "TFNSW_API_KEY",
    ) -> None:
        super().__init__(
            APIConfig(
                base_url=base_url,
                api_key_env=api_key_env,
                default_headers={"Accept": "application/octet-stream"},
            )
        )

    def fetch_feed(self, feed_code: str) -> pd.DataFrame:
        """Fetch a realtime GTFS feed payload."""

        logger.info("Requesting TfNSW realtime feed: %s", feed_code)
        content = self.get_bytes(feed_code)
        now = datetime.utcnow()
        df = pd.DataFrame(
            [
                {
                    "feed_code": feed_code,
                    "retrieved_at": now,
                    "gtfs_realtime": content,
                }
            ]
        )
        return df
