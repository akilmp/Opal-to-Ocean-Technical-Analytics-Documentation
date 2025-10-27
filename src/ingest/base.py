"""Base HTTP client utilities for data ingestion."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional

import requests

logger = logging.getLogger(__name__)


@dataclass
class APIConfig:
    """Configuration for API clients."""

    base_url: str
    api_key_env: Optional[str] = None
    api_key_header: str = "Authorization"
    api_key_prefix: str = "Bearer "
    default_headers: Optional[Mapping[str, str]] = None


class BaseAPIClient:
    """Simple requests-based API client with optional API key support."""

    def __init__(self, config: APIConfig) -> None:
        self.config = config
        self.session = requests.Session()
        self._api_key = None
        if config.api_key_env:
            self._api_key = os.getenv(config.api_key_env)
            if not self._api_key:
                logger.warning(
                    "Environment variable %s is not set; requests may be unauthenticated",
                    config.api_key_env,
                )

    # Public -----------------------------------------------------------------
    def get_json(
        self,
        endpoint: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        timeout: int = 30,
    ) -> Dict[str, Any]:
        response = self._request(
            "GET", endpoint, params=params, headers=headers, timeout=timeout
        )
        return response.json()

    def get_bytes(
        self,
        endpoint: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        timeout: int = 60,
    ) -> bytes:
        response = self._request(
            "GET", endpoint, params=params, headers=headers, timeout=timeout
        )
        return response.content

    # Private ----------------------------------------------------------------
    def _compose_headers(
        self, headers: Optional[Mapping[str, str]] = None
    ) -> Dict[str, str]:
        merged: Dict[str, str] = {}
        if self.config.default_headers:
            merged.update(dict(self.config.default_headers))
        if headers:
            merged.update(dict(headers))
        if self._api_key:
            merged.setdefault(
                self.config.api_key_header,
                f"{self.config.api_key_prefix}{self._api_key}",
            )
        return merged

    def _request(
        self,
        method: str,
        endpoint: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        timeout: int,
    ) -> requests.Response:
        url = self._build_url(endpoint)
        response = self.session.request(
            method,
            url,
            params=params,
            headers=self._compose_headers(headers),
            timeout=timeout,
        )
        try:
            response.raise_for_status()
        except requests.HTTPError:
            body = None
            try:
                body = response.json()
            except ValueError:
                body = response.text
            logger.error("Request to %s failed: %s", url, body)
            raise
        return response

    def _build_url(self, endpoint: str) -> str:
        if endpoint.startswith("http://") or endpoint.startswith("https://"):
            return endpoint
        return f"{self.config.base_url.rstrip('/')}/{endpoint.lstrip('/')}"


class CKANClient(BaseAPIClient):
    """Utility client for CKAN-backed NSW data portals."""

    def fetch_all_records(
        self,
        resource_id: str,
        *,
        limit: int = 1000,
        params: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Fetch all records for a CKAN resource."""

        records = []
        offset = 0
        total = None
        params = dict(params or {})
        while True:
            page_params = dict(params)
            page_params.update({"resource_id": resource_id, "limit": limit, "offset": offset})
            payload = self.get_json("", params=page_params)
            result = payload.get("result", {})
            page_records = result.get("records", [])
            records.extend(page_records)
            total = result.get("total", len(records))
            offset += len(page_records)
            if not page_records or offset >= total:
                break
        return {"records": records, "total": total or len(records)}

    def fetch_records_df(
        self,
        resource_id: str,
        *,
        limit: int = 1000,
        params: Optional[Mapping[str, Any]] = None,
    ):
        from pandas import DataFrame

        data = self.fetch_all_records(resource_id, limit=limit, params=params)
        records = data["records"]
        if not records:
            return DataFrame()
        return DataFrame.from_records(records)


def encode_filters(filters: Mapping[str, Any]) -> str:
    """Encode CKAN filters for datastore_search queries."""

    return json.dumps(filters)
