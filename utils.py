from google.cloud import storage
import tempfile
import os
import json


def get_mediastack_api_key(bucket_file_name, bucket_name):
    storage_client = storage.Client()
    blob = [blob for blob in storage_client.list_blobs(bucket_name) if blob.name == bucket_file_name][0]

    with tempfile.TemporaryDirectory() as td:
        f_name = os.path.join(td, bucket_file_name)
        with open(f_name, 'wb') as file:
            storage_client.download_blob_to_file(
                blob, file
            )
        with open(f_name, 'rb') as file:
            key_data = json.load(file)
    return key_data