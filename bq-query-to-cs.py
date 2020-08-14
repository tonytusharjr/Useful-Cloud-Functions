#!/usr/bin/env python3

# Import modules
import os
import google.auth
from google.oauth2 import service_account
from google.cloud import bigquery


def bq_query_to_cs(request):

    # To use this file locally set $IS_LOCAL=1 and populate environment variable $GOOGLE_APPLICATION_CREDENTIALS with path to keyfile
    # Get Application Default Credentials if running in Cloud Functions
    if os.getenv("IS_LOCAL") is None:
        credentials, project = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"])
    else:
        credentials = service_account.Credentials.from_service_account_file(
            os.getenv("GOOGLE_APPLICATION_CREDENTIALS"), scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

    client = bigquery.Client(credentials=credentials)

    # Query a table(s) and write the results to a new table
    job_config = bigquery.QueryJobConfig()
    table_ref = client.dataset("<dataset-name>").table("<table-name>")
    job_config.destination = table_ref
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    QUERY = (
        'SELECT column1, column2, column3,'
        '...'
        'FROM `project.dataset.table`'
        'WHERE maybe some conditional goes here'
    )
    query_job = client.query(QUERY)
    rows = query_job.result()  # Waits for the query to finish
    print(rows)

    # Export query result table to cloud storage
    destination_uri = "gs://<bucket-name>/<filename>"
    dataset_ref = client.dataset("<dataset-name>", project="<project-id>")
    table_ref = dataset_ref.table("<table-name>")

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        location='US')
    extract_job.result()  # Waits for job to complete

    # Delete table after export to GCS
    table_id = '<project-id>.<dataset-name>.<table-created-in-query>'

    client.delete_table(table_id, not_found_ok=True)
    print("Deleted table '{}'.".format(table_id))
