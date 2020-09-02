#!/usr/bin/env python

# from dotenv import load_dotenv
import boto3  # module for interacting with AWS s3
import os
import glob  # module for scanning directory files
from google.cloud import storage
from google.cloud import secretmanager

"""
For local development prior cloud deploy
# load env variables
load_dotenv()
# declare local json key file for gcp authentication
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'localpath/key.json'
"""

# variables for AWS Client Authentication
# AWS access-key-id
project_id = 'project-id'  # GCS project-id, not AWS
secret_id_a = 'access-key-id'  # Need to setup two secrets in Secret Manager
secret_version = 1
# setup secret manager and grab api key for function
client = secretmanager.SecretManagerServiceClient()
name = client.secret_version_path(project_id, secret_id_a, secret_version)
response_a = client.access_secret_version(name)
secret_string_a = response_a.payload.data.decode('UTF-8')

# AWS secret-access-key
project_id = 'project-id'
secret_id_b = 'secret-access-key'
secret_version = 1
# setup secret manager and grab api key for function
client = secretmanager.SecretManagerServiceClient()
name = client.secret_version_path(project_id, secret_id_b, secret_version)
response_b = client.access_secret_version(name)
secret_string_b = response_b.payload.data.decode('UTF-8')

# authenticate AWS
client = boto3.client(
    's3',
    aws_access_key_id=secret_string_a,
    aws_secret_access_key=secret_string_b
)

# AWS bucketpath variables
s3_bucket_name = 's3-bucket'
export_user = 'subdirectory-path'  # Interesting scenario: s3 bucket managed by third-party,
# I have read-only access to a 'sub-directory' path in the bucket
local_path = '/tmp/'
remoteDirectoryName = export_user + '/'

# list all objects in bucket folders
s3 = boto3.resource('s3')


def main(request):
    def download_dir(prefix, local, bucket, client=client):
        prefix = remoteDirectoryName
        local = local_path
        bucket = s3_bucket_name
        client = client

        keys = []
        dirs = []
        next_token = ''
        base_kwargs = {
            'Bucket': bucket,
            'Prefix': prefix,
        }
        while next_token is not None:
            kwargs = base_kwargs.copy()
            if next_token != '':
                kwargs.update({'ContinuationToken': next_token})
            results = client.list_objects_v2(**kwargs)
            contents = results.get('Contents')
            for i in contents:
                k = i.get('Key')
                if k[-1] != '/':
                    keys.append(k)
                else:
                    dirs.append(k)
            next_token = results.get('NextContinuationToken')
        for d in dirs:
            dest_pathname = os.path.join(local, d)
            if not os.path.exists(os.path.dirname(dest_pathname)):
                os.makedirs(os.path.dirname(dest_pathname))
        for k in keys:
            dest_pathname = os.path.join(local, k)
            if not os.path.exists(os.path.dirname(dest_pathname)):
                os.makedirs(os.path.dirname(dest_pathname))
            client.download_file(bucket, k, dest_pathname)

    download_dir(remoteDirectoryName, local_path, s3_bucket_name, client)

    gcs_bucket_name = 'gcs-bucket'
    gcs_path = 'path from gs://'
    gcs_client = storage.Client()
    bucket = gcs_client.bucket(gcs_bucket_name)

    def upload_local_directory_to_gcs(local_path, bucket, gcs_path):
        assert os.path.isdir(local_path)
        for local_file in glob.glob(local_path + '/**'):
            if not os.path.isfile(local_file):
                upload_local_directory_to_gcs(local_file, bucket, gcs_path + "/" + os.path.basename(local_file))
            else:
                remote_path = os.path.join(gcs_path, local_file[1 + len(local_path):])
                blob = bucket.blob(remote_path)
                blob.upload_from_filename(local_file)

    upload_local_directory_to_gcs(local_path, bucket, gcs_path)
