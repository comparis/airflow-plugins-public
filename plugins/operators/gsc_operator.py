from __future__ import annotations

import json
from datetime import datetime
from tempfile import NamedTemporaryFile
from typing import Any, Callable, List, Mapping, Sequence

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.context import Context

from hooks.gsc_hook import GscHook


def get_data_availability(**kwargs) -> bool:
    hook = GscHook(
        gcp_conn_id=kwargs['gcp_conn_id'],
        impersonation_chain=kwargs['impersonation_chain']
    )

    data_availability = hook.get_data_availability(site_url=kwargs['site_url'],
                                                   start_date=kwargs['date'],
                                                   end_date=kwargs['date'],
                                                   data_state=kwargs['data_state'])

    return 'rows' in data_availability and len(data_availability['rows']) > 0


class GscDataAvailabilitySensor(PythonSensor):
    """Sensor for Google Search Console data availability."""

    def __init__(
            self,
            python_callable: Callable = get_data_availability,
            op_kwargs: Mapping[str, Any] | None = None,
            **kwargs):
        super().__init__(
            python_callable=python_callable,
            op_kwargs=op_kwargs,
            **kwargs)


class GoogleSearchConsoleToGcsOperator(GoogleCloudBaseOperator):
    template_fields = ['gsc_gcp_conn_id', 'gsc_impersonation_chain', 'date',
                       'gcs_gcp_conn_id', 'gcs_bucket', 'gcs_filepath']

    def __init__(
            self,
            gsc_gcp_conn_id: str = 'google_search_console_default',
            gsc_impersonation_chain: str | Sequence[str] | None = None,
            site_url: str = None,
            date: str = None,
            aggregation_type: str = None,
            dimensions: List[str] = None,
            types: List[str] = None,
            data_state: str = None,
            gcs_gcp_conn_id: str = 'google_cloud_default',
            gcs_bucket: str = None,
            gcs_filepath: str = None,
            **kwargs):
        super().__init__(**kwargs)
        self.gsc_gcp_conn_id = gsc_gcp_conn_id
        self.gsc_impersonation_chain = gsc_impersonation_chain

        self.site_url = site_url
        self.date = date
        self.aggregation_type = aggregation_type
        self.dimensions = dimensions
        self.types = types
        self.data_state = data_state

        self.gcs_gcp_conn_id = gcs_gcp_conn_id
        self.gcs_bucket = gcs_bucket
        self.gcs_filepath = gcs_filepath

    def execute(self, context: Context) -> None:
        gsc_hook = GscHook(
            gcp_conn_id=self.gsc_gcp_conn_id,
            impersonation_chain=self.gsc_impersonation_chain
        )

        gcs_hook = GCSHook(
            gcp_conn_id=self.gcs_gcp_conn_id
        )

        with NamedTemporaryFile('w') as tmp_file:
            self._write_data_to_file(gsc_hook, tmp_file)

            self.log.info(f'Uploading {tmp_file.name} to gs://{self.gcs_bucket}/{self.gcs_filepath}')
            gcs_hook.upload(self.gcs_bucket, self.gcs_filepath, filename=tmp_file.name,
                            mime_type="application/json; charset=utf-8")

    def _write_data_to_file(self, gsc_hook: GscHook, tmp_file: NamedTemporaryFile) -> None:
        for type in self.types:
            row_limit = 25000
            start_row = 0

            while True:
                self.log.info(f'Fetching rows from {start_row} to {start_row + row_limit}...')
                result = gsc_hook.get_data(self.site_url, self.date, self.date, self.dimensions,
                                           self.aggregation_type, type, self.data_state, start_row, row_limit)

                if 'rows' not in result or len(result['rows']) == 0:
                    self.log.info('Stopping here, no rows to fetch.')
                    break

                for row in result['rows']:
                    searchanalytics_data = {
                        'date': self.date,
                        'property': self.site_url,
                        'type': type,
                        'data_state': self.data_state,
                        'dimensions': self.dimensions,
                        'keys': row['keys'],
                        'clicks': row['clicks'],
                        'impressions': row['impressions'],
                        'position': row['position'],
                        'ctr': row['ctr'],
                        'load_date': datetime.utcnow().isoformat()
                    }

                    json.dump(searchanalytics_data, tmp_file)
                    tmp_file.write('\n')

                row_count = len(result["rows"])
                self.log.info(f'Fetched {row_count} rows.')
                tmp_file.flush()

                if row_count < row_limit:
                    self.log.info('Stopping here, no more data to fetch.')
                    break

                start_row += row_limit
