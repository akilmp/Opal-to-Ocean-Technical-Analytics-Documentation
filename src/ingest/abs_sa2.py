"""Client for ABS SA2 contextual datasets."""

from __future__ import annotations

import logging
import os
from typing import Mapping, Optional

import pandas as pd

from .base import APIConfig, CKANClient, encode_filters

logger = logging.getLogger(__name__)

DEFAULT_ABS_BASE_URL = "https://data.gov.au/data/api/3/action/datastore_search"


class ABSSA2ContextClient(CKANClient):
    """Client for ABS SA2 context via the Australian open data portal."""

    def __init__(
        self,
        *,
        base_url: str = DEFAULT_ABS_BASE_URL,
        api_key_env: str = "DATA_GOV_AU_API_KEY",
        dataset_env: str = "ABS_SA2_DATASET_ID",
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
                f"Environment variable {self.dataset_env} must be set with the ABS SA2 resource id"
            )
        return dataset_id

    def fetch_context(
        self,
        *,
        limit: int = 1000,
        filters: Optional[Mapping[str, object]] = None,
    ) -> pd.DataFrame:
        """Fetch contextual attributes for SA2 regions."""

        params = {}
        if filters:
            params["filters"] = encode_filters(filters)
        dataset_id = self._dataset_id()
        logger.info("Fetching ABS SA2 context records for dataset %s", dataset_id)
        return self.fetch_records_df(dataset_id, limit=limit, params=params)
