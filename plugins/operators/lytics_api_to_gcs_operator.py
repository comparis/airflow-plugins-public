"""

Lytics API to Google Cloud Storage transfer operator

"""

import json
import uuid
import logging
from datetime import datetime
from tempfile import NamedTemporaryFile

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.exceptions import AirflowFailException

from hooks.lytics_api_hook import LyticsAPIHook

log = logging.getLogger(__name__)


class LyticsAPIToGoogleCloudStorage(BaseOperator):

    template_fields = ['lytics_conn_id', 'gcp_conn_id', 'gcs_bucket', 'gcs_filepath']

    def __init__(
            self,
            lytics_conn_id='lytics_api_default',
            lytics_api_path=None,
            gcp_conn_id='google_cloud_default',
            gcs_bucket=None,
            gcs_filepath=None,
            properties=None,
            *args, **kwargs):
        super(LyticsAPIToGoogleCloudStorage, self).__init__(*args, **kwargs)
        self.lytics_conn_id = lytics_conn_id
        self.lytics_api_path = lytics_api_path
        self.gcp_conn_id = gcp_conn_id
        self.gcs_bucket = gcs_bucket
        self.gcs_filepath = gcs_filepath
        self.properties = properties

    def execute(self, context):
        # initialize hooks to Lytics and GCS
        lytics_api_hook = LyticsAPIHook(
            lytics_conn_id=self.lytics_conn_id
        )
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id
        )

        with NamedTemporaryFile("w") as f:
            records = []
            if self.lytics_api_path == "/v2/job":
                get_v2_job_r= lytics_api_hook.get_v2_job(show_deleted=True, show_completed=True, check_http_error=True)
                get_v2_job = json.loads(get_v2_job_r.text)

                for data in get_v2_job["data"]:
                    records.append({
                        "timestamp": str(datetime.utcnow()),
                        "data": data
                    })
            elif self.lytics_api_path == "/v2/job/{id}/logs":
                get_v2_job_r= lytics_api_hook.get_v2_job(show_deleted=False, show_completed=False, check_http_error=True)
                get_v2_job = json.loads(get_v2_job_r.text)

                for get_v2_job_data in get_v2_job["data"]:
                    id = get_v2_job_data["id"]
                    get_v2_job_logs_r= lytics_api_hook.get_v2_job_logs(id, check_http_error=True)
                    get_v2_job_logs = json.loads(get_v2_job_logs_r.text)

                    for get_v2_job_logs_data in get_v2_job_logs["data"]:
                        records.append({
                            "timestamp":str(datetime.utcnow()),
                            "data": get_v2_job_logs_data
                        })
            elif self.lytics_api_path == "/api/ml":
                get_v1_ml_r= lytics_api_hook.get_v1_ml(check_http_error=True)
                get_v1_ml = json.loads(get_v1_ml_r.text)

                for data in get_v1_ml["data"]:
                    records.append({
                        "timestamp": str(datetime.utcnow()),
                        "data": data
                    })
            elif self.lytics_api_path == "/api/ml/{id}/summary":
                get_v1_ml_r= lytics_api_hook.get_v1_ml(check_http_error=True)
                get_v1_ml = json.loads(get_v1_ml_r.text)

                for get_v1_ml_data in get_v1_ml["data"]:
                    id = get_v1_ml_data["id"]
                    get_v1_ml_summary_r= lytics_api_hook.get_v1_ml_summary(id, check_http_error=True)
                    get_v1_ml_summary = json.loads(get_v1_ml_summary_r.text)
                    
                    records.append({
                        "timestamp": str(datetime.utcnow()),
                        "data": get_v1_ml_summary["data"]
                    })
            elif self.lytics_api_path == "/api/segment/sizes":
                if self.properties is None:
                    raise AirflowFailException(f"Missing required properties for API path {self.lytics_api_path}")

                response = lytics_api_hook.get_v1_segment_sizes(self.properties["audiences"])
                results = json.loads(response.text)["data"]

                if results is not None:
                    for result in results:
                        records.append({
                            "timestamp": str(datetime.utcnow()),
                            "data": result
                        })
            elif self.lytics_api_path == "/v2/stream":
                response = lytics_api_hook.get_v2_stream()
                results = json.loads(response.text)["data"]

                if results is not None:
                    for result in results:
                        records.append({
                            "timestamp": str(datetime.utcnow()),
                            "data": result
                        })
            else:
                raise AirflowFailException(f"Unsupported API path {self.lytics_api_path}")
            
            for record in records:
                # dump each data record to temp file
                json.dump(record, f)
                f.write('\n')
    
            f.flush()
            gcs_hook.upload(self.gcs_bucket, self.gcs_filepath, filename=f.name, mime_type="application/json; charset=utf-8")

# FIXME: legacy: not needed as replaced by LyticsAPIToGoogleCloudStorage
class LyticsMLAPIToGoogleCloudStorage(BaseOperator):

    template_fields = ['lytics_conn_id', 'gcp_conn_id', 'gcs_bucket', 'gcs_filepath']

    def __init__(
            self,
            lytics_conn_id='lytics_api_default',
            gcp_conn_id='google_cloud_default',
            gcs_bucket=None,
            gcs_filepath=None,
            *args, **kwargs):
        super(LyticsMLAPIToGoogleCloudStorage, self).__init__(*args, **kwargs)
        self.lytics_conn_id = lytics_conn_id
        self.gcp_conn_id = gcp_conn_id
        self.gcs_bucket = gcs_bucket
        self.gcs_filepath = gcs_filepath

    def execute(self, context):
        # initialize hooks to MSSQL and Lytics
        lytics_api_hook = LyticsAPIHook(
            lytics_conn_id=self.lytics_conn_id
        )
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id
        )

        with NamedTemporaryFile("w") as f:
            # retrieve ml model list from API
            get_v1_ml_r= lytics_api_hook.get_v1_ml(check_http_error=True)
            get_v1_ml = json.loads(get_v1_ml_r.text)["data"]

            for ml_model_details in get_v1_ml:
                ml_model_state = dict()

                # add model details to dict
                ml_model_state.update(ml_model_details)

                # retrieve summary data for the model by its id
                get_v1_ml_summary_r = lytics_api_hook.get_v1_ml_summary(model_id=ml_model_details["id"], check_http_error=True)
                get_v1_ml_summary = json.loads(get_v1_ml_summary_r.text)["data"]

                # special handling for source and target predictions as these cannot be loaded as records
                if "source_predictions" in get_v1_ml_summary["summary"]:
                    get_v1_ml_summary["summary"]["source_predictions"] = json.dumps(get_v1_ml_summary["summary"]["source_predictions"])
                if "target_predictions" in get_v1_ml_summary["summary"]:
                    get_v1_ml_summary["summary"]["target_predictions"] = json.dumps(get_v1_ml_summary["summary"]["target_predictions"])

                # add model summary to dict
                ml_model_state.update(get_v1_ml_summary)

                # dump ml model records to temp file
                json.dump(ml_model_state, f)
                f.write('\n')
    
            f.flush()
            gcs_hook.upload(self.gcs_bucket, self.gcs_filepath, filename=f.name, mime_type="application/json; charset=utf-8")
