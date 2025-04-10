""" 
### Description

Iterable API Hook

"""
import logging
from urllib.parse import quote_plus

import tenacity
from airflow.providers.http.hooks.http import HttpHook
from airflow.exceptions import AirflowFailException


class IterableAPIHook(HttpHook):

    # FIXME: add retries for http calls regarding connection error

    def __init__(self, 
            itr_conn_id='iterable_api_default'):
        super(IterableAPIHook, self).__init__(
            http_conn_id=None
        )
        self.log.setLevel(logging.WARNING)

        itr_conn = self.get_connection(itr_conn_id)

        self.itr_base_url = itr_conn.host
        self.itr_api_key = itr_conn.password

        self.retry_args = dict(
            wait=tenacity.wait_random_exponential(max=60),
            stop=tenacity.stop_after_attempt(10)
        )

    def bulk_update_users(self, bulk_update_users_request, check_http_error=False):
        """
        Executes https://api.iterable.com/api/docs#users_bulkUpdateUsers

        :param bulk_update_users_request: BulkUpdateUsersRequest
        :type bulk_update_users_request: dict
        """

        self.method = 'POST'
        response = self.run_with_advanced_retry(
            endpoint=f"{self.itr_base_url}/api/users/bulkUpdate",
            json = bulk_update_users_request,
            headers = {
                "Content-Type": "application/json",
                "Api-Key": self.itr_api_key
            },
            extra_options = {
                "check_response": check_http_error,
                "verify": True
            },
            _retry_args=self.retry_args
        )
        
        return response

    def users_delete(self, user_email, check_http_error=False):
        """
        Executes https://api.iterable.com/api/docs#users_delete

        :param user_email: email of user to delete
        :type user_email: string
        """

        encoded_user_email = quote_plus(user_email)

        self.method = 'DELETE'
        response = self.run_with_advanced_retry(
            endpoint=f"{self.itr_base_url}/api/users/{encoded_user_email}",
            headers = {
                "Api-Key": self.itr_api_key
            },
            extra_options = {
                "check_response": check_http_error,
                "verify": True
            },
            _retry_args=self.retry_args
        )

        return response

    def bulk_subscription_action(self, subscription_group, subscription_group_id, action, bulk_subscription_action_request, check_http_error=False):
        """
        Executes https://api.iterable.com/api/docs#subscriptions_Bulk_subscription_action

        :param subscription_group: Subscription group (emailList, messageType, messageChannel)
        :type subscription_group: string
        :param subscription_group_id: Subscription Group Id
        :type subscription_group_id: string
        :param action: action to take (subscribe/unsubscribe)
        :type action: string
        :param bulk_subscription_action_request: BulkSubscriptionActionRequest
        :type bulk_subscription_action_request: dict
        """

        self.method = 'PUT'
        response = self.run_with_advanced_retry(
            endpoint=f"{self.itr_base_url}/api/subscriptions/{subscription_group}/{subscription_group_id}",
            params = {
                "action": action
            },
            json = bulk_subscription_action_request,
            headers = {
                "Content-Type": "application/json",
                "Api-Key": self.itr_api_key
            },
            extra_options = {
                "check_response": check_http_error,
                "verify": True
            },
            _retry_args=self.retry_args
        )
        
        return response

    def campaigns(self, check_http_error=True):
        """
        Executes https://api.iterable.com/api/docs#campaigns_campaigns

        """

        self.method = 'GET'
        response = self.run_with_advanced_retry(
            endpoint=f"{self.itr_base_url}/api/campaigns",
            headers = {
                "Api-Key": self.itr_api_key
            },
            extra_options = {
                "check_response": check_http_error,
                "verify": True
            },
            _retry_args=self.retry_args
        )
        
        return response

    def message_types(self, check_http_error=True):
        """
        Executes https://api.iterable.com/api/docs#messageTypes_messageTypes

        """

        self.method = 'GET'
        response = self.run_with_advanced_retry(
            endpoint=f"{self.itr_base_url}/api/messageTypes",
            headers = {
                "Api-Key": self.itr_api_key
            },
            extra_options = {
                "check_response": check_http_error,
                "verify": True
            },
            _retry_args=self.retry_args
        )
        
        return response

    def templates(self, template_type, message_medium, check_http_error=True):
        """
        Executes https://api.iterable.com/api/docs#templates_getTemplates

        """

        if template_type not in ["Base", "Blast", "Triggered", "Workflow"]:
            raise AirflowFailException("template type not supported")

        if message_medium not in ["Email", "Push", "InApp", "SMS"]:
            raise AirflowFailException("message medium not supported")

        self.method = 'GET'
        response = self.run_with_advanced_retry(
            endpoint=f"{self.itr_base_url}/api/templates",
            data = {
                "templateType": template_type,
                "messageMedium": message_medium,
            },
            headers = {
                "Api-Key": self.itr_api_key
            },
            extra_options = {
                "check_response": check_http_error,
                "verify": True
            },
            _retry_args=self.retry_args
        )
        
        return response

    def email_template(self, template_id, check_http_error=True):
        """
        Executes https://api.iterable.com/api/docs#templates_getEmailTemplate

        """

        self.method = 'GET'
        response = self.run_with_advanced_retry(
            endpoint=f"{self.itr_base_url}/api/templates/email/get",
            data = {
                "templateId": template_id
            },
            headers = {
                "Api-Key": self.itr_api_key
            },
            extra_options = {
                "check_response": check_http_error,
                "verify": True
            },
            _retry_args=self.retry_args
        )
        
        return response

    def channels(self, check_http_error=True):
        """
        Executes https://api.iterable.com/api/docs#channels_channels

        """

        self.method = 'GET'
        response = self.run_with_advanced_retry(
            endpoint=f"{self.itr_base_url}/api/channels",
            headers = {
                "Api-Key": self.itr_api_key
            },
            extra_options = {
                "check_response": check_http_error,
                "verify": True
            },
            _retry_args=self.retry_args
        )
        
        return response

    def catalogs(self, page=1, page_size=10000, check_http_error=True):
        """
        Executes https://api.iterable.com/api/docs#catalogs_listCatalogs

        """

        self.method = 'GET'
        response = self.run_with_advanced_retry(
            endpoint=f"{self.itr_base_url}/api/catalogs",
            data = {
                "page": page,
                "pageSize": page_size
            },
            headers = {
                "Api-Key": self.itr_api_key
            },
            extra_options = {
                "check_response": check_http_error,
                "verify": True
            },
            _retry_args=self.retry_args
        )
        
        return response

    def catalog_items(self, catalog_name, page=1, page_size=10000, check_http_error=True):
        """
        Executes https://api.iterable.com/api/docs#catalogs_listCatalogItems

        """

        self.method = 'GET'
        response = self.run_with_advanced_retry(
            endpoint=f"{self.itr_base_url}/api/catalogs/{catalog_name}/items",
            data = {
                "page": page,
                "pageSize": page_size
            },
            headers = {
                "Api-Key": self.itr_api_key
            },
            extra_options = {
                "check_response": check_http_error,
                "verify": True
            },
            _retry_args=self.retry_args
        )
        
        return response

    def export_data_json(self, data_type_name, start_date_time=None, end_date_time=None, only_fields=None, check_http_error=True):
        """
        Executes https://api.iterable.com/api/docs#export_exportDataJson
        """
        
        self.method = 'GET'
        response = self.run_with_advanced_retry(
            endpoint=f"{self.itr_base_url}/api/export/data.json",
            data = {
                "range": 'All',
                "dataTypeName": data_type_name,
                "startDateTime": start_date_time,
                "endDateTime": end_date_time,
                "onlyFields": only_fields
            },
            headers = {
                "Api-Key": self.itr_api_key
            },
            extra_options = {
                "check_response": check_http_error,
                "verify": True,
                "stream": True # responses here can be large
            },
            _retry_args=self.retry_args
        )
        
        return response