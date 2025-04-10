"""

BigQuery Table Schema to Google Cloud Storage export operator

"""

import logging
import json
from tempfile import NamedTemporaryFile

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook


log = logging.getLogger(__name__)


class BigQueryTableSchemaToGoogleCloudStorage(BaseOperator):

    template_fields = [
        'source_dataset_id', 
        'source_table_id', 
        'destination_gcs_bucket', 
        'destination_gcs_filepath', 
        'gcp_conn_id', 
        'impersonation_chain'
    ]

    def __init__(
            self,
            source_dataset_id, 
            source_table_id,
            destination_gcs_bucket=None,
            destination_gcs_filepath=None,
            gcp_conn_id='google_cloud_default', 
            impersonation_chain=None,
            *args, **kwargs):
        super(BigQueryTableSchemaToGoogleCloudStorage, self).__init__(*args, **kwargs)
        self.source_dataset_id = source_dataset_id
        self.source_table_id = source_table_id
        self.destination_gcs_bucket = destination_gcs_bucket
        self.destination_gcs_filepath = destination_gcs_filepath
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

        log.setLevel(logging.INFO)

    def execute(self, context):
        # initialize hooks
        bq_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            use_legacy_sql=False
        )
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain
        )

        # get schema
        schema = bq_hook.get_schema(dataset_id=self.source_dataset_id, table_id=self.source_table_id)

        # write to GCS
        with NamedTemporaryFile("w") as f:
            json.dump(schema, f)
            f.flush()
            gcs_hook.upload(
                self.destination_gcs_bucket, 
                self.destination_gcs_filepath, 
                filename=f.name, 
                mime_type="application/json; charset=utf-8"
            )

