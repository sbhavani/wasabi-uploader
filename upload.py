from minio import Minio
from minio.error import S3Error
import argparse
import json

def load_secrets(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)
    
def upload_file_to_wasabi(endpoint, access_key, secret_key, bucket_name, file_path, object_name):
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

    # Upload the file to the bucket.
    try:
        client.fput_object(
            bucket_name,
            object_name,
            file_path,
        )
        print(f"'{file_path}' is successfully uploaded as object '{object_name}' to bucket '{bucket_name}'.")
    except S3Error as e:
        print(f"Error occurred: {e}")

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
