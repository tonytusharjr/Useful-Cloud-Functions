#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import packages/modules
from google.cloud import storage
from google.cloud import secretmanager
import os
import json

# setup scret manager, pull secret key for api wrapper
# PREREQUISITE: must give GCP service account the 'Secret Manager Secret Accessor' role !!!
client = secretmanager.SecretManagerServiceClient()
secret_name = "<secret-api-key>" # define this in cloud shell command during project setup
project_id = "<project-id>"
resource_name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
response = client.access_secret_version(resource_name)
secret_string = response.payload.data.decode('UTF-8')

# api wrapper variables, differs depending on project you are doing
apikey = secret_string
baseuri = DEFAULT_BASE_URI


# define main function in cloud function, in this case...
# ...make API call using an API wrapper package...
# ...get dict response, convert to JSON, write to tmp directory...
# ...upload file to storage bucket
def main(request):

    # configure api key use for session at hand
    <my.api.wrapper>.configure(apikey, baseuri)

    # get data on beers as a dict - returns one of many pages...
    # ...TODO create a loop for calling all pages and appending
    data = <my.api.wrapper.some.wrapper.function()>

    # prints the response dict (with single quotes)
    print(data)

    # convert returned dict to json
    data_json = json.dumps(data)

    # prints the json type (with double quotes)
    print(data_json)

    # provide path and filename to with open as outfile:
    with open('/tmp/' + 'data_json.json', 'w') as outfile:
        json.dump(data_json, outfile)

    # function for uploading to cloud storage
    def upload_blob(file_name):
        bucket_name = '<bucket-name>'
        storage_client = storage.Client("<project-id>")
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.upload_from_filename(file_name)

        print('BEGINNING EXPORT.')

    # call function for upload
    upload_blob('<file-name>')
