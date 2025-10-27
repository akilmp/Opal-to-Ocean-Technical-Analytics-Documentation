"""Client for City of Sydney pedestrian counts."""

from __future__ import annotations

import logging
import os
from typing import Mapping, Optional

import pandas as pd

from .base import APIConfig, CKANClient, encode_filters

logger = logging.getLogger(__name__)

DEFAULT_CITY_DATA_BASE_URL = (
    "https://data.cityofsydney.nsw.gov.au/api/3/action/datastore_search"
)


class CityOfSydneyPedestrianClient(CKANClient):
    """Client for the City of Sydney pedestrian counters dataset."""

    def __init__(
        self,
        *,
        base_url: str = DEFAULT_CITY_DATA_BASE_URL,
        api_key_env: str = "CITY_OF_SYDNEY_API_KEY",
        dataset_env: str = "CITY_PEDESTRIAN_DATASET_ID",
    ) -> None:
        self.dataset_env = dataset_env
        super().__init__(
            APIConfig(
                base_url=base_url,
                api_key_env=api_key_env,
                api_key_prefix="Bearer ",
                api_key_header="Authorization",
            )
        )

    def _dataset_id(self) -> str:
        dataset_id = os.getenv(self.dataset_env)
        if not dataset_id:
            raise ValueError(
                f"Environment variable {self.dataset_env} must be set with the pedestrian counts resource id"
            )
        return dataset_id

    def fetch_counts(
        self,
        *,
        limit: int = 1000,
        filters: Optional[Mapping[str, object]] = None,
    ) -> pd.DataFrame:
        """Fetch pedestrian count observations."""

        params = {}
        if filters:
            params["filters"] = encode_filters(filters)
        dataset_id = self._dataset_id()
        logger.info("Fetching City of Sydney pedestrian records for dataset %s", dataset_id)
        return self.fetch_records_df(dataset_id, limit=limit, params=params)
