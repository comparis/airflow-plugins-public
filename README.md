# Comparis Public Airflow Plugins

This is mirror of some of the operators and hooks that we have developed for our internal use, 
hoping it would be useful for the rest of the community.

## Local Development (uv)

Create the virutal environment and install all dependencies:
```
uv python install 3.11
```

Then, activate the virtual environment:
```
uv sync
```

## How to use

Place the plugins in the `plugins` folder of your Airflow installation. The plugins will be automatically loaded by Airflow. 
You can use symlink or copy the files directly into the `plugins` folder.

## Available Plugins

### Hooks
- **LyticsAPIHook**  
  Hook for interacting with the Lytics API endpoints with built-in retry logic.
- **IterableAPIHook**  
  Hook for calling Iterable API resources (users, campaigns, templates, exports) with retries.
- **GscHook**  
  Hook for Google Search Console to query data availability and analytics.

### Operators & Sensors
- **RestrictHourSensor**  
  Sensor that waits until the current UTC hour falls within a specified window.
- **LyticsAPIToGoogleCloudStorage**  
  Fetches data from Lytics API paths and writes newline-delimited JSON to GCS.
- **IterableCampaignsAPIToGoogleCloudStorage**  
  Retrieves Iterable campaigns and uploads them as JSON to GCS.
- **IterableChannelsAPIToGoogleCloudStorage**  
  Retrieves Iterable channels and uploads them as JSON to GCS.
- **IterableMessageTypesAPIToGoogleCloudStorage**  
  Retrieves Iterable message types and uploads them as JSON to GCS.
- **IterableEmailTemplateAPIToGoogleCloudStorage**  
  Fetches Iterable email templates within a date range and uploads them as JSON to GCS.
- **IterableCatalogAPIToGoogleCloudStorage**  
  Retrieves Iterable catalog items and uploads them as JSON to GCS.
- **IterablePurchaseAPIToGoogleCloudStorage**  
  Exports Iterable purchase data, adds a hashed userId, and uploads JSON to GCS.
- **IterableUserAPIToGoogleCloudStorage**  
  Exports Iterable user data with selected fields and uploads newline-delimited JSON to GCS.
- **GscDataAvailabilitySensor**  
  PythonSensor that checks for data availability in Google Search Console.
- **GoogleSearchConsoleToGcsOperator**  
  Fetches search analytics from Google Search Console and uploads newline-delimited JSON to GCS.
- **BigQueryInsertJobOperatorWrapper**  
  Extension of BigQueryInsertJobOperator that supports reading SQL queries from local files or GCS.
- **BigQueryTableSchemaToGoogleCloudStorage**  
  Exports a BigQuery table’s schema as JSON and writes it to GCS.

