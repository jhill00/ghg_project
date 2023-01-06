from google.cloud import storage
import os

# authenticate ourselves
service_key = '.\GCP\service_key.json'
client = storage.Client.from_service_account_json(json_credentials_path = service_key)

## create Google Cloud Storage (GCS) bucket
# bucket_name = desired name for GCS bucket
def create_gcs_bucket(bucket_name: str):

    b = client.bucket(bucket_name)
    
    if b.exists():
        print(f'{bucket_name} already exists.')
    else:
        bucket = client.create_bucket(bucket_name)
        print(f'GCS bucket [{bucket.name}] has been created.')

## upload files to GCS
# bucket_name = name of GCS bucket to load files to
# path = directory of files to upload
def upload_files_to_gcs(bucket_name: str, path: str):

    bucket = storage.Bucket(client, bucket_name)

    if os.listdir(path):
        for file in os.listdir(path):
            blob = bucket.blob(file)
            blob.upload_from_filename('\\'.join([path, file]))
    else:
        print('There are no files in specified directory.')