#!/usr/bin/env python3

# export IS_LOCAL=1
# export GOOGLE_APPLICATION_CREDENTIALS=<local-path-to-json-key-file>

# Import modules
import os
import google.auth
from google.oauth2 import service_account
from google.cloud import bigquery


def bq_table_to_cs(request):
    # To use this file locally set $IS_LOCAL=1 and populate environment variable $GOOGLE_APPLICATION_CREDENTIALS with path to keyfile
    # Get Application Default Credentials if running in Cloud Functions
    if os.getenv("IS_LOCAL") is None:
        credentials, project = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"])
    else:
        credentials = service_account.Credentials.from_service_account_file(
            os.getenv("GOOGLE_APPLICATION_CREDENTIALS"), scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
    # Replace these values according to your project
    project_name = "<project-id>"
    bucket_name = "<bucket-name>"
    file_name = "<export-file-name>"
    dataset_name = "<bq-dataset-name>"
    table_name = "<bq-table-name>"
    destination_uri = "gs://{}/{}".format(bucket_name, file_name)

    bq_client = bigquery.Client(project=project_name, credentials=credentials)

    dataset = bq_client.dataset(dataset_name, project=project_name)
    table_to_export = dataset.table(table_name)

    job_config = bigquery.job.ExtractJobConfig()
    # job_config.compression = bigquery.Compression.GZIP

    extract_job = bq_client.extract_table(
        table_to_export,
        destination_uri,
        # Location must match that of the source table.
        location="US",
        job_config=job_config,
    )
    extract_job.result()
    print(
        "Job with ID {} started exporting data from {}.{} to {}".format(
            extract_job.job_id, dataset_name, table_name, destination_uri)
    )
