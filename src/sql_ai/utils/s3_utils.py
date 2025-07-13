from typing import Optional

import boto3
import botocore

from sql_ai.utils.utils import (
    find_aws_profile_by_account_id,
    get_all_files_in_directory,
)


def print_bucket_size(s3, bucket_name: str):
    """
    Prints the total size of all objects in the specified bucket
    in both bytes and megabytes.
    """
    total_size_bytes = 0
    continuation_token = None

    while True:
        if continuation_token:
            response = s3.list_objects_v2(
                Bucket=bucket_name, ContinuationToken=continuation_token
            )
        else:
            response = s3.list_objects_v2(Bucket=bucket_name)

        for obj in response.get("Contents", []):
            total_size_bytes += obj["Size"]

        if response.get("IsTruncated"):
            continuation_token = response["NextContinuationToken"]
        else:
            break

    total_size_mb = round(total_size_bytes / (1024 * 1024), 2)

    print(f"Bucket size: {total_size_bytes} bytes, ({total_size_mb} MB)")


def ensure_bucket_exists(s3, bucket_name, region="eu-west-2"):
    """
    Check if the specified bucket exists and create it if it does not.

    Args:
        s3 (boto3.client.S3): The S3 client.
        bucket_name (str): The name of the S3 bucket.
        region (str): The AWS region where to create the bucket.
    """
    try:
        s3.head_bucket(Bucket=bucket_name)
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response["Error"]["Code"])
        if error_code == 404:
            print(f"Bucket '{bucket_name}' does not exist. Creating it...")
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        else:
            raise  # Some other error


def upload_file_to_s3(s3, bucket_name, subfolders, file_path, object_key):
    """
    Uploads a file to a specified subfolder in an S3 bucket.

    Parameters:
    - s3: boto3 S3 client instance.
    - bucket_name (str): Name of the S3 bucket.
    - subfolders (str): Subfolder path within the S3 bucket (e.g., 'folder1/folder2').
    - file_path (str): Local path to the file to be uploaded.
    - object_key (str): Name of the file once uploaded to S3 (e.g., 'data.csv').

    The final S3 object will be uploaded to: s3://{bucket_name}/{subfolder}/{object_key}

    Raises:
    - Prints an error message if the upload fails.
    """
    if subfolders:
        s3_location = f"s3://{bucket_name}/{subfolders}/{object_key}"
    else:
        s3_location = f"s3://{bucket_name}/{object_key}"
    try:
        s3.upload_file(file_path, bucket_name, f"{subfolders.rstrip('/')}/{object_key}")
        print(f"Uploaded {object_key} to {s3_location}")
    except Exception as e:
        print(f"Failed to upload '{file_path}' to {s3_location}: {e}")


def load_files_to_s3(
    bucket_name: str,
    file_directory: str,
    bucket_subfolder: Optional[str] = None,
    file_type: str = ".html",
    should_list_size_of_files_in_bucket: bool = True,
    bucket_region="eu-west-2",
    max_minutes_ago_to_save=None,
):
    """
    Load and upload files from a local directory to an S3 bucket.

    This function initializes a boto3 session and ensures the specified S3 bucket
    exists. It then retrieves all files from a specified directory and uploads
    each one to the S3 bucket with a timestamped object key i.e. inside a relevant
    yyyy-mm-dd directory.

    The progress of the uploads is printed to the console, including the number of
    files processed and confirmation of each successful upload.
    """

    session = boto3.Session(profile_name=find_aws_profile_by_account_id("382901073838"))
    s3 = session.client("s3", bucket_region)

    ensure_bucket_exists(s3, bucket_name, region=bucket_region)

    files = get_all_files_in_directory(
        directory_path=file_directory,
        suffix=file_type,
        modified_within_minutes=max_minutes_ago_to_save,
    )

    print(f"Starting upload of {len(files)} files to {bucket_name}")

    for i, file in enumerate(files):
        file_path = file[0]
        file_name = file[1]

        upload_file_to_s3(
            s3=s3,
            bucket_name=bucket_name,
            subfolders=bucket_subfolder,
            file_path=file_path,
            object_key=file_name,
        )

        print(f"{i+1}/{len(files)} files processed = {round((i+1)/len(files)*100, 2)}%")

    if should_list_size_of_files_in_bucket:
        print_bucket_size(s3=s3, bucket_name=bucket_name)
