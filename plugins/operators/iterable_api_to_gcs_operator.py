"""

Iterable API to Google Cloud Storage transfer operator

"""
import hashlib
import json
import logging
from datetime import datetime
from datetime import timezone
from tempfile import NamedTemporaryFile

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from hooks.iterable_api_hook import IterableAPIHook

log = logging.getLogger(__name__)


class IterableCampaignsAPIToGoogleCloudStorage(BaseOperator):

    template_fields = ['itr_conn_id', 'gcp_conn_id', 'gcs_bucket', 'gcs_filepath']

    def __init__(
            self,
            itr_conn_id='iterable_api_default', 
            gcp_conn_id='google_cloud_default',
            gcs_bucket=None,
            gcs_filepath=None,
            *args, **kwargs):
        super(IterableCampaignsAPIToGoogleCloudStorage, self).__init__(*args, **kwargs)
        self.itr_conn_id = itr_conn_id
        self.gcp_conn_id = gcp_conn_id
        self.gcs_bucket = gcs_bucket
        self.gcs_filepath = gcs_filepath

    def execute(self, context):
        # initialize hooks to Iterable and GCS
        iterable_api_hook = IterableAPIHook(
            itr_conn_id=self.itr_conn_id
        )
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id
        )

        # fetch JSON data from API 
        data_r = iterable_api_hook.campaigns()
        campaigns = json.loads(data_r.text)["campaigns"]

        # compute increment
        records = []
        for campaign in campaigns:
            records.append(campaign)
        
        # convert records array to newline delimited json and upload to gcs
        with NamedTemporaryFile("w") as f:
            for record in records:
                json.dump(record, f)
                f.write('\n')
            f.flush()
            gcs_hook.upload(self.gcs_bucket, self.gcs_filepath, filename=f.name, mime_type="application/json; charset=utf-8")


class IterableChannelsAPIToGoogleCloudStorage(BaseOperator):

    template_fields = ['itr_conn_id', 'gcp_conn_id', 'gcs_bucket', 'gcs_filepath']

    def __init__(
            self,
            itr_conn_id='iterable_api_default', 
            gcp_conn_id='google_cloud_default',
            gcs_bucket=None,
            gcs_filepath=None,
            *args, **kwargs):
        super(IterableChannelsAPIToGoogleCloudStorage, self).__init__(*args, **kwargs)
        self.itr_conn_id = itr_conn_id
        self.gcp_conn_id = gcp_conn_id
        self.gcs_bucket = gcs_bucket
        self.gcs_filepath = gcs_filepath

    def execute(self, context):
        # initialize hooks to Iterable and GCS
        iterable_api_hook = IterableAPIHook(
            itr_conn_id=self.itr_conn_id
        )
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id
        )
        
        # fetch JSON data from API 
        data_r = iterable_api_hook.channels()
        records = json.loads(data_r.text)["channels"]

        # convert records array to newline delimited json and upload to gcs
        with NamedTemporaryFile("w") as f:
            for record in records:
                json.dump(record, f)
                f.write('\n')
            f.flush()
            gcs_hook.upload(self.gcs_bucket, self.gcs_filepath, filename=f.name, mime_type="application/json; charset=utf-8")


class IterableMessageTypesAPIToGoogleCloudStorage(BaseOperator):

    template_fields = ['itr_conn_id', 'gcp_conn_id', 'gcs_bucket', 'gcs_filepath']

    def __init__(
            self,
            itr_conn_id='iterable_api_default', 
            gcp_conn_id='google_cloud_default',
            gcs_bucket=None,
            gcs_filepath=None,
            *args, **kwargs):
        super(IterableMessageTypesAPIToGoogleCloudStorage, self).__init__(*args, **kwargs)
        self.itr_conn_id = itr_conn_id
        self.gcp_conn_id = gcp_conn_id
        self.gcs_bucket = gcs_bucket
        self.gcs_filepath = gcs_filepath

    def execute(self, context):
        # initialize hooks to Iterable and GCS
        iterable_api_hook = IterableAPIHook(
            itr_conn_id=self.itr_conn_id
        )
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id
        )
        
        # fetch JSON data from API 
        data_r = iterable_api_hook.message_types()
        records = json.loads(data_r.text)["messageTypes"]

        # convert records array to newline delimited json and upload to gcs
        with NamedTemporaryFile("w") as f:
            for record in records:
                json.dump(record, f)
                f.write('\n')
            f.flush()
            gcs_hook.upload(self.gcs_bucket, self.gcs_filepath, filename=f.name, mime_type="application/json; charset=utf-8")


class IterableEmailTemplateAPIToGoogleCloudStorage(BaseOperator):

    template_fields = ['itr_conn_id', 'gcp_conn_id', 'gcs_bucket', 'gcs_filepath', 'updated_at_start_date', 'updated_at_end_date']

    def __init__(
            self,
            itr_conn_id='iterable_api_default', 
            gcp_conn_id='google_cloud_default',
            gcs_bucket=None,
            gcs_filepath=None,
            updated_at_start_date=None, # Accepts yyyy-MM-ddTHH:mm:ss+00:00
            updated_at_end_date=None, # Accepts yyyy-MM-ddTHH:mm:ss+00:00
            *args, **kwargs):
        super(IterableEmailTemplateAPIToGoogleCloudStorage, self).__init__(*args, **kwargs)
        self.itr_conn_id = itr_conn_id
        self.gcp_conn_id = gcp_conn_id
        self.gcs_bucket = gcs_bucket
        self.gcs_filepath = gcs_filepath
        self.updated_at_start_date = updated_at_start_date
        self.updated_at_end_date = updated_at_end_date

    def execute(self, context):
        # initialize hooks to Iterable and GCS
        iterable_api_hook = IterableAPIHook(
            itr_conn_id=self.itr_conn_id
        )
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id
        )
        
        # fetch JSON template data from API 
        templates = []
        template_types = ["Base", "Blast", "Triggered", "Workflow"]
        message_medium = "Email"
        for template_type in template_types:
            data_r = iterable_api_hook.templates(
                template_type=template_type,message_medium=message_medium)
            templates.extend(
                json.loads(data_r.text)["templates"])

        # fetch JSON email template data from API 
        records = []
        for template in templates:
            template_id = template["templateId"]
            updated_at = datetime.fromtimestamp(template["updatedAt"] / 1000.0, tz=timezone.utc) # updatedAt is in timestamp millis
            if updated_at >= datetime.fromisoformat(self.updated_at_start_date) and \
                    updated_at < datetime.fromisoformat(self.updated_at_end_date):
                data_r = iterable_api_hook.email_template(
                    template_id=template_id)
                record = json.loads(data_r.text)
                record["createdAt"] = template["createdAt"] # email template createdAt is project template createdAt
                record["updatedAt"] = template["updatedAt"] # email template updatedAt is project template updatedAt
                records.append(record)

        # convert records array to newline delimited json and upload to gcs
        with NamedTemporaryFile("w") as f:
            for record in records:
                json.dump(record, f)
                f.write('\n')
            f.flush()
            gcs_hook.upload(self.gcs_bucket, self.gcs_filepath, filename=f.name, mime_type="application/json; charset=utf-8")


class IterableCatalogAPIToGoogleCloudStorage(BaseOperator):

    template_fields = ['itr_conn_id', 'gcp_conn_id', 'gcs_bucket', 'gcs_filepath']

    def __init__(
            self,
            itr_conn_id='iterable_api_default', 
            gcp_conn_id='google_cloud_default',
            gcs_bucket=None,
            gcs_filepath=None,
            *args, **kwargs):
        super(IterableCatalogAPIToGoogleCloudStorage, self).__init__(*args, **kwargs)
        self.itr_conn_id = itr_conn_id
        self.gcp_conn_id = gcp_conn_id
        self.gcs_bucket = gcs_bucket
        self.gcs_filepath = gcs_filepath

    def execute(self, context):
        # initialize hooks to Iterable and GCS
        iterable_api_hook = IterableAPIHook(
            itr_conn_id=self.itr_conn_id
        )
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id
        )
        
        # fetch JSON catalog data from API 
        # FIXME: we should iterate over pages, fix this long term! 
        data_r = iterable_api_hook.catalogs()
        catalog_names = json.loads(data_r.text)["params"]["catalogNames"]

        # fetch JSON email template data from API 
        records = []
        for catalog_name in catalog_names:
            # FIXME: we should iterate over pages, fix this long term! 
            data_r = iterable_api_hook.catalog_items(catalog_name["name"])
            catalog_items = json.loads(data_r.text)["params"]["catalogItemsWithProperties"]
            for catalog_item in catalog_items:
                record = {
                    "catalogName": catalog_item["catalogName"],
                    "itemId": catalog_item["itemId"],
                    "size": catalog_item["size"],
                    "lastModified": catalog_item["lastModified"],
                    "value": json.dumps(catalog_item["value"]) # JSON type field with variable schema per item
                }
                records.append(record)

        # convert records array to newline delimited json and upload to gcs
        with NamedTemporaryFile("w") as f:
            for record in records:
                json.dump(record, f)
                f.write('\n')
            f.flush()
            gcs_hook.upload(self.gcs_bucket, self.gcs_filepath, filename=f.name, mime_type="application/json; charset=utf-8")


class IterablePurchaseAPIToGoogleCloudStorage(BaseOperator):

    template_fields = ['itr_conn_id', 'gcp_conn_id', 'gcs_bucket', 'gcs_filepath', 'start_date_time', 'end_date_time']

    def __init__(
            self,
            start_date_time, # Accepts yyyy-MM-dd HH:mm:ss
            end_date_time, # Accepts yyyy-MM-dd HH:mm:ss
            gcs_bucket,
            gcs_filepath,
            itr_conn_id='iterable_api_default', 
            gcp_conn_id='google_cloud_default',
            *args, **kwargs):
        super(IterablePurchaseAPIToGoogleCloudStorage, self).__init__(*args, **kwargs)
        self.itr_conn_id = itr_conn_id
        self.gcp_conn_id = gcp_conn_id
        self.gcs_bucket = gcs_bucket
        self.gcs_filepath = gcs_filepath
        self.start_date_time = start_date_time
        self.end_date_time = end_date_time

    def execute(self, context):
        # initialize hooks to Iterable and GCS
        iterable_api_hook = IterableAPIHook(
            itr_conn_id=self.itr_conn_id
        )
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id
        )
        
        data_r = iterable_api_hook.export_data_json(
            data_type_name='purchase',
            start_date_time=self.start_date_time,
            end_date_time=self.end_date_time,
            check_http_error=True
        )

        records = []
        for row_str in data_r.text.splitlines():
            row = json.loads(row_str)
            row['userId'] = hashlib.sha256(row['email'].encode('utf-8').strip().lower()).hexdigest().lower()

            records.append(row)

        # convert records array to newline delimited json and upload to gcs
        with NamedTemporaryFile("w") as f:
            for record in records:
                json.dump(record, f)
                f.write('\n')
            f.flush()
            gcs_hook.upload(self.gcs_bucket, self.gcs_filepath, filename=f.name, mime_type="application/json; charset=utf-8")


class IterableUserAPIToGoogleCloudStorage(BaseOperator):

    template_fields = ['itr_conn_id', 'gcp_conn_id', 'gcs_bucket', 'gcs_filepath']

    def __init__(
            self,
            fields,
            gcs_bucket,
            gcs_filepath,
            itr_conn_id='iterable_api_default',
            gcp_conn_id='google_cloud_default',
            *args, **kwargs):
        super(IterableUserAPIToGoogleCloudStorage, self).__init__(*args, **kwargs)
        self.itr_conn_id = itr_conn_id
        self.gcp_conn_id = gcp_conn_id
        self.gcs_bucket = gcs_bucket
        self.gcs_filepath = gcs_filepath
        self.fields = fields

    def execute(self, context):
        # initialize hooks to Iterable and GCS
        iterable_api_hook = IterableAPIHook(
            itr_conn_id=self.itr_conn_id
        )
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id
        )
        
        data_r = iterable_api_hook.export_data_json(
            data_type_name="user",
            only_fields=self.fields,
            check_http_error=True
        )

        # convert records array to newline delimited json (with buffer of 100kb to avoid OOM) and upload to gcs
        with NamedTemporaryFile("w", buffering=102400) as f:
            # read lines to temporary file
            for record_str in data_r.iter_lines(1000):
                # read json record from response
                record = json.loads(record_str)

                # write newline-json record directly to file
                json.dump(record, f)
                f.write('\n')
            f.flush()

            # upload file to gcs
            gcs_hook.upload(self.gcs_bucket, self.gcs_filepath, filename=f.name, mime_type="application/json; charset=utf-8")
