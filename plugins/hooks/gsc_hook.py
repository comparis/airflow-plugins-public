from __future__ import annotations

import logging
from typing import Any, List, Sequence

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from googleapiclient.discovery import build


class GscHook(GoogleBaseHook):
    """
    Hook for Google Search Console.

    Docu: https://developers.google.com/webmaster-tools/v1/searchanalytics/query#request
    """

    _conn: build | None = None

    def __init__(
            self,
            api_version: str = "v1",
            # To access the Google Search Console API, the specific
            # scope "https://www.googleapis.com/auth/webmasters.readonly" is required.
            # Hence, a custom connection is necessary.
            gcp_conn_id: str = "google_search_console_default",
            impersonation_chain: str | Sequence[str] | None = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
        )
        self.api_version = api_version
        self.log.setLevel(logging.WARNING)

    def get_conn(self) -> Any:
        """Retrieves a connection to the Google Search Console."""

        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build(
                "searchconsole",
                self.api_version,
                http=http_authorized,
                cache_discovery=False,
            )
        return self._conn

    def get_data_availability(self, site_url: str, start_date: str, end_date: str, data_state: str) -> dict:
        """Check if data is available."""

        response = self.get_conn().searchanalytics().query(
            siteUrl=site_url,
            body={
                'startDate': start_date,
                'endDate': end_date,
                'dimensions': ['date'],
                'dataState': data_state
            }
        ).execute(num_retries=self.num_retries)

        return response

    def get_data(self, site_url: str, start_date: str, end_date: str, dimensions: List[str], aggregation_type: str,
                 type: str, data_state: str, start_row: int, row_limit: int) -> dict:
        """Get Google Search Console data."""

        response = self.get_conn().searchanalytics().query(
            siteUrl=site_url,
            body={
                'startDate': start_date,
                'endDate': end_date,
                'dimensions': dimensions,
                'aggregationType': aggregation_type,
                'type': type,
                'dataState': data_state,
                'startRow': start_row,
                'rowLimit': row_limit
            }
        ).execute(num_retries=self.num_retries)

        return response
