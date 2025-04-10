"""

Wrappers for BigQuery operators.

"""

from typing import Sequence
from urllib.parse import urlparse

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


class BigQueryInsertJobOperatorWrapper(BigQueryInsertJobOperator):
    """
    Wrapper over default operator to execute a BigQuery job.

    This operator extends default features with the following features:
        - Read .sql files from gs:// path and add it to the configuration
        - Read .sql files from local path and add it to the configuration
    """


    template_ext: Sequence[str] = (
        # this setting allows to automatically read files with allowed extentions and template them
        # we only support .json files here, as we we have our custom logic to read the .sql files from gs:// path
        # and add it to the configuration
        ".json",
    )
    
    def pre_execute(self, context):
        """
        This hook is triggered right before self.execute() is called.
        """

        try:
            # Read .sql files passed to configuration.query.query, which also 
            # supports templating as field configuration.query.query is in template_fields

            if (
                # if refering to the .sql file in gs:// 
                "query" in self.configuration and \
                "query" in self.configuration["query"] and \
                "gs://" in self.configuration["query"]["query"] and \
                ".sql" in self.configuration["query"]["query"]
            ):
                self.log.info("Reading sql query from remote: %s'", self.configuration["query"]["query"])
                query = self._download_query(self.configuration["query"]["query"])
                self.configuration["query"]["query"] = query
            elif (
                # if refering to the .sql file in local disk
                "query" in self.configuration and \
                "query" in self.configuration["query"] and \
                ".sql" in self.configuration["query"]["query"]
            ):
                self.log.info("Reading sql query from local: %s'", self.configuration["query"]["query"])
                with open(self.configuration["query"]["query"]) as file:
                    self.configuration["query"]["query"] = file.read()
            
        except Exception as e:
            self.log.exception(e)

    def _download_query(self, sql_file_url) -> str:
        parsed_url = urlparse(sql_file_url, allow_fragments=False)
        bucket_name = parsed_url.netloc
        object_name = parsed_url.path.lstrip('/')

        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain
        )

        sql_query = gcs_hook.download(
            bucket_name=bucket_name,
            object_name=object_name
        )
        
        return sql_query.decode("utf-8")