# Useful-Cloud-Functions

A few GCP Cloud Function scenarios I've ran into:

* bq-query-to-cs: Queries a BigQuery table, writes a new table, downloads query-generated table as CSV, deletes query-generated table
* bq-table-to-cs: Export job of a BigQuery table, similar to above function except no SQL query in the Python script as the query is handled internally in BigQuery as a scheduled query
* cs-to-cs: Download source file to temp folder from a Cloud Storage bucket, perform a transformation, upload transformed file to a Cloud Storage bucket
* call-to-cs: Use API wrapper to get dict response, write to a JSON file, upload file to cloud storage for ELT

# Todo

* Add an API call scenario
* Add how to locally test Cloud Functions

# Dependencies

* Fill this in...

# Notes
