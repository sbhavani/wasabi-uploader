from minio import Minio
from minio.error import S3Error
import argparse
import json
import os
from tqdm import tqdm
import time
from urllib3.exceptions import ProtocolError
import threading

class ProgressPercentage:
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()
        self._pbar = tqdm(total=self._size, unit='B', unit_scale=True, desc=f"Uploading {filename}")

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            self._pbar.update(bytes_amount)

    def set_meta(self, object_name, total_length):
        pass  # This method is required but we don't need to do anything here

    def update(self, bytes_amount):
        self.__call__(bytes_amount)

def load_secrets(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)
    
def upload_file_to_wasabi(endpoint, access_key, secret_key, bucket_name, file_path, object_name, max_retries=3):
    # Initialize the MinIO client object.
    client = Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=True  # Wasabi uses HTTPS by default
    )

    # Make the bucket if it does not exist.
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' created")
    else:
        print(f"Bucket '{bucket_name}' already exists")

    file_size = os.path.getsize(file_path)

    for attempt in range(max_retries):
        try:
            progress = ProgressPercentage(file_path)
            with open(file_path, 'rb') as file_data:
                result = client.put_object(
                    bucket_name,
                    object_name,
                    file_data,
                    file_size,
                    progress=progress
                )
            print(f"\n'{file_path}' is successfully uploaded as object '{object_name}' to bucket '{bucket_name}'.")
            print(f"Etag: {result.etag}, Version ID: {result.version_id}")
            return
        except (S3Error, ProtocolError) as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"\nUpload attempt {attempt + 1} failed. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"\nError occurred after {max_retries} attempts: {e}")
                raise

if __name__ == "__main__":
    # Create an argument parser
    parser = argparse.ArgumentParser(description="Upload a file to Wasabi S3 storage.")
    
    # Add the file_path argument
    parser.add_argument("file_path", type=str, help="Path to the local file to upload")
    
    # Parse the arguments
    args = parser.parse_args()

    # Replace these with your Wasabi server details
    endpoint = "s3.us-central-1.wasabisys.com"  # Wasabi endpoint

    # Load secrets from the JSON file
    secrets = load_secrets('secrets.json')
    access_key = secrets['access_key']
    secret_key = secrets['secret_key']

    bucket_name = "pardis"
    object_name = args.file_path.split('/')[-1]  # This is the name of the object in the bucket

    upload_file_to_wasabi(endpoint, access_key, secret_key, bucket_name, args.file_path, object_name)
