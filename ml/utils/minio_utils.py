from botocore.exceptions import ClientError
from boto3 import resource
from mypy_boto3_s3.service_resource import S3ServiceResource, Bucket
from dotenv import load_dotenv
import os


load_dotenv()


def get_minio_resource() -> S3ServiceResource:
    return resource(
        's3',
        endpoint_url=os.getenv('MINIO_ENDPOINT'),
        aws_access_key_id=os.getenv('MINIO_ROOT_USER'),
        aws_secret_access_key=os.getenv('MINIO_ROOT_PASSWORD'),
        region_name='us-east-1'
    )


def get_or_create_bucket(bucket_name: str | None) -> Bucket:
    """
    Retrieve an existing bucket or create a new one in MinIO/S3.

    This function connects to MinIO using the configured resource, checks if a bucket
    with the given name exists, and creates it if it does not exist. Raises an error
    if `bucket_name` is None.

    Args:
        bucket_name (str | None): Name of the bucket to retrieve or create.

    Returns:
        Bucket: A Boto3 Bucket resource object corresponding to the given bucket name.

    Raises:
        ValueError: If `bucket_name` is None or invalid.
        botocore.exceptions.ClientError: If bucket creation fails due to other reasons
            (e.g., permissions or network errors).
    """
    minio_client = get_minio_resource()
    if bucket_name is None:
        print("Bucket is not valid")
        raise ValueError("Bucket is not valid")
    if minio_client.Bucket(bucket_name) not in minio_client.buckets.all():
        minio_client.create_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} created")
    return minio_client.Bucket(bucket_name)


def object_exists(bucket, key: str) -> bool:
    """
    Check if an object exists in a bucket.

    Args:
        bucket: Boto3 Bucket resource
        key (str): Object key to check

    Returns:
        bool: True if exists, False otherwise
    """
    obj = bucket.Object(key)
    try:
        obj.load()  # tries to get object metadata
        return True
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code')
        if error_code == '404':
            return False
        else:
            raise


def upload_file_to_bucket(bucket: Bucket, file_path: str, target_path: str | None = None) -> None:
    """
    Upload a file to a MinIO bucket.

    Args:
        bucket (Bucket): The MinIO bucket to upload to.
        file_path (str): Path to the file to upload.
        target_path (str | None, optional): Target path in the bucket. If None, the file name is used.
    """
    if not os.path.exists(file_path):
        print(f"File {file_path} does not exist")
        return
    if object_exists(bucket, file_path):
        print(f"File {file_path} already exists in bucket {bucket.name}")
        return
    bucket.upload_file(file_path, target_path or file_path)
    print(f"File {file_path} uploaded to bucket {bucket.name}")


def list_objects_in_bucket(bucket_name: str, file_name: str | None = None) -> list[str]:
    """
    List all objects in a MinIO bucket.

    Args:
        bucket (Bucket): The MinIO bucket to list objects from.
    """
    bucket = get_or_create_bucket(bucket_name)
    file_paths = [
        f"s3a://{bucket_name}/{obj.key}"
        for obj in bucket.objects.all()
        if file_name is None or file_name in obj.key
    ]
    return file_paths
