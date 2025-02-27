import os
import mimetypes
import pathlib
from google.cloud import storage
from google.oauth2 import service_account

STORAGE_CLASSES = ('STANDARD', 'NEARLINE', 'COLDLINE', 'ARCHIVE')
class GCStorage:
    def __init__(self, storage_client):
        self.client = storage_client

    def create_bucket(self, bucket_name, storage_class, bucket_location='US'):
        bucket = self.client.bucket(bucket_name)
        bucket.storage_class = storage_class
        return self.client.create_bucket(bucket, bucket_location)
    
    def get_bucket(self, bucket_name):
        return self.client.get_bucket(bucket_name)
    
    def list_buckets(self):
        buckets = self.client.list_buckets()
        return [bucket.name for bucket in buckets]

    def upload_file(self, bucket, blob_destination, file_path):
        file_type = file_path.split()[-1]
        if file_type == 'csv':
            content_type = 'text/csv'
        elif file_type == 'psd':
            content_type = 'image/vnd.adobe.photoshop'
        else:
            content_type = mimetypes.guess_type(file_path)[0]
        blob = bucket.blob(blob_destination)
        blob.upload_from_filename(file_path, content_type=content_type)
        return blob
    
    def list_blobs(self, bucket_name):
        return self.client.list_blobs(bucket_name)

credentials_path = '/opt/airflow/plugins/optical-torch-452002-c8-d798135ff968.json'
project_id = 'optical-torch-452002-c8'
bucket_name = 'nexar_event_bucket'
credentials = service_account.Credentials.from_service_account_file(credentials_path)

# Prepare the variables
working_dir = pathlib.Path.cwd()
#files_folder = working_dir.joinpath('raw_data')
files_folder = pathlib.Path('/opt/airflow/plugins/raw_data')

#downloads_folder = working_dir.join_path()

# Construct GCStorage instance
storage_client = storage.Client(credentials=credentials, project=project_id)
gcs = GCStorage(storage_client)

# Create Cloud Storage bucket
#if not bucket_name in gcs.list_buckets():
#    bucket_gcs = gcs.create_bucket('nexar_event_bucket', STORAGE_CLASSES[0])
#else:
bucket_gcs = gcs.get_bucket(bucket_name)

print(bucket_gcs)
# Upload Files
for file_path in files_folder.glob('*.*'):
    # use full file path
    gcs.upload_file(bucket_gcs, file_path.name, str(file_path))
    print("OKEEEEEEE")

# Download and Delete Files
#gcs_demo_blobs = gcs.list_blobs('')
#for blob in gcs_demo_blobs:
#    path_download = downloads_folder.joinpath(blob.name)
#    if not path_download.parent.exists():
#        path_download.parent.mkdir(parents=True)
#    blob.download_to_filename(str(path_download))
#    blob.delete()

