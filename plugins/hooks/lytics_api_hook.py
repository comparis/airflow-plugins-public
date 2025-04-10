""" 
### Description

Lytics API Hook

"""

import logging
from urllib.parse import quote_plus

import tenacity
from airflow.providers.http.hooks.http import HttpHook


class LyticsAPIHook(HttpHook):

    # FIXME: add retries for http calls regarding connection error

    def __init__(self, 
            lytics_conn_id='lytics_api_default'):
        super(LyticsAPIHook, self).__init__(
            http_conn_id=None
        )
        self.log.setLevel(logging.WARNING)

        self.lytics_conn = self.get_connection(lytics_conn_id)

        self.retry_args = dict(
            wait=tenacity.wait_random_exponential(max=60),
            stop=tenacity.stop_after_attempt(10)
        )

    def get_v2_job(self, show_completed=False, show_deleted=False, check_http_error=False):
        """
        Executes Get Jobs - https://docs.lytics.com/reference/get_job
        """

        self.method = 'GET'
        response = self.run_with_advanced_retry(
            endpoint=f"{self.lytics_conn.host}/v2/job",
            data={
                "show_completed": "true" if show_completed else "false",
                "show_deleted": "true" if show_deleted else "false",
            },
            headers = {
                "Authorization": self.lytics_conn.password
            },
            extra_options = {
                "check_response": check_http_error,
                "verify": True
            },
            _retry_args=self.retry_args
        )
        
        return response
    
    def get_v2_job_logs(self, id, check_http_error=False):
        """
        Executes Get job logs - https://docs.lytics.com/reference/get_job-id-logs
        """

        self.method = 'GET'
        response = self.run_with_advanced_retry(
            endpoint=f"{self.lytics_conn.host}/v2/job/{id}/logs",
            headers = {
                "Authorization": self.lytics_conn.password
            },
            extra_options = {
                "check_response": check_http_error,
                "verify": True
            },
            _retry_args=self.retry_args
        )
        
        return response
    
    def get_v1_ml(self, check_http_error=False):
        """
        Executes ML/List - https://learn.lytics.com/documentation/developer/api-docs/ml#ml-list-ml-list-get
        """

        self.method = 'GET'
        response = self.run_with_advanced_retry(
            endpoint=f"{self.lytics_conn.host}/api/ml",
            headers = {
                "Authorization": self.lytics_conn.password
            },
            extra_options = {
                "check_response": check_http_error,
                "verify": True
            },
            _retry_args=self.retry_args
        )
        
        return response

    def get_v1_ml_summary(self, model_id, check_http_error=False):
        """
        Executes ML/ModelSummary - https://learn.lytics.com/documentation/developer/api-docs/ml#ml-summary-ml-model-summary-fetch-get
        """

        self.method = 'GET'
        response = self.run_with_advanced_retry(
            endpoint=f"{self.lytics_conn.host}/api/ml/{model_id}/summary",
            headers = {
                "Authorization": self.lytics_conn.password
            },
            extra_options = {
                "check_response": check_http_error,
                "verify": True
            },
            _retry_args=self.retry_args
        )
        
        return response

    def get_v1_entity_user_email(self, email, fields=["email"], meta=False, segments=False, download=False, check_http_error=False):
        """
        https://learn.lytics.com/documentation/developer/api-docs/personalization#personalization-personalization-get
        """

        email_encoded = quote_plus(email)

        self.method = 'GET'
        response = self.run_with_advanced_retry(
            endpoint=f"{self.lytics_conn.host}/api/entity/user/email/{email_encoded}",
            data={
                "fields": fields,
                "meta": "true" if meta else "false",
                "segments": "true" if segments else "false",
                "download": "true" if download else "false",
            },
            headers = {
                "Authorization": self.lytics_conn.password
            },
            extra_options = {
                "check_response": check_http_error,
                "verify": True
            },
            _retry_args=self.retry_args
        )
        
        return response

    def delete_v1_entity_user_email(self, email, check_http_error=False):
        """
        https://learn.lytics.com/documentation/developer/api-docs/personalization#profile-deletion-profile-delete-request-delete
        """

        email_encoded = quote_plus(email)

        self.method = 'DELETE'
        response = self.run_with_advanced_retry(
            endpoint=f"{self.lytics_conn.host}/api/entity/user/email/{email_encoded}",
            headers = {
                "Authorization": self.lytics_conn.password
            },
            extra_options = {
                "check_response": check_http_error,
                "verify": True
            },
            _retry_args=self.retry_args
        )
        
        return response

    def get_v1_entity_deletestatus(self, del_req_id, check_http_error=False):
        """
        https://learn.lytics.com/documentation/developer/api-docs/personalization#profile-deletion-profile-deletion-status-get
        """

        self.method = 'GET'
        response = self.run_with_advanced_retry(
            endpoint=f"{self.lytics_conn.host}/api/entity/deletestatus/{del_req_id}",
            headers = {
                "Authorization": self.lytics_conn.password
            },
            extra_options = {
                "check_response": check_http_error,
                "verify": True
            },
            _retry_args=self.retry_args
        )
        
        return response

    def get_v1_segment_sizes(self, ids, check_http_error=False):
        """
        https://learn.lytics.com/documentation/developer/api-docs/segment#segment-sizes-segment-sizes-get
        """

        self.method = 'GET'
        response = self.run_with_advanced_retry(
            endpoint=f"{self.lytics_conn.host}/api/segment/sizes",
            data={
                "ids": ids,
            },
            headers={
                "Authorization": self.lytics_conn.password
            },
            extra_options={
                "check_response": check_http_error,
                "verify": True
            },
            _retry_args=self.retry_args
        )

        return response

    def get_v2_stream(self, check_http_error=False):
        """
        https://docs.lytics.com/reference/get_stream
        """

        self.method = 'GET'
        response = self.run_with_advanced_retry(
            endpoint=f"{self.lytics_conn.host}/v2/stream",
            headers={
                "Authorization": self.lytics_conn.password
            },
            extra_options={
                "check_response": check_http_error,
                "verify": True
            },
            _retry_args=self.retry_args
        )

        return response
