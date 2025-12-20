from dotenv import load_dotenv
import os
from pathlib import Path
import shutil
import sys
import wget
from zipfile import ZipFile

from ..utils.minio_utils import upload_file_to_bucket, get_or_create_bucket


env_path = os.path.join(Path(__file__).parent.parent.absolute(), ".env")
load_dotenv(env_path)


def download_dataset(dataset_url: str, local_dir: str, filename: str = "temp.zip") -> str:
    """
    Download a dataset from a URL into the local directory, but skip if the file already exists.

    Args:
        dataset_url (str): URL of the dataset to download.
        local_dir (str): Directory to save the downloaded file.
        filename (str, optional): Name of the file to save. Defaults to "temp.zip".

    Returns:
        str: Full path to the downloaded file.
    """
    # Ensure the directory exists
    os.makedirs(local_dir, exist_ok=True)

    # Full file path
    file_path = os.path.join(local_dir, filename)

    # Skip download if file exists
    if os.path.exists(file_path):
        print(f"File already exists at {file_path}, skipping download.")
    else:
        print(f"Downloading {dataset_url} to {file_path} ...")
        wget.download(dataset_url, file_path)
        print("\nDownload complete.")

    return file_path


def unzip_dataset(file_path: str, target_path: str) -> None:
    """
    Unzip a dataset into the target directory, skipping extraction if already unzipped.

    Args:
        file_path (str): Path to the zip file.
        target_path (str): Directory where the contents should be extracted.
    """
    # Ensure target directory exists
    os.makedirs(target_path, exist_ok=True)
    existing_files = [
        os.path.join(target_path, f)
        for f in os.listdir(target_path)
        if os.path.isfile(os.path.join(target_path, f)) and os.path.join(target_path, f) != file_path
    ]
    # Skip extraction if directory is not empty
    if existing_files:
        print(f"Target directory '{target_path}' is not empty. Skipping unzip.")
        return

    print(f"Extracting {file_path} to {target_path} ...")
    with ZipFile(file_path, "r") as zip_ref:
        zip_ref.extractall(target_path)
    print("Extraction complete.")


def upload_dataset_to_minio(bucket_name, folder_path: str) -> None:
    bucket = get_or_create_bucket(bucket_name)
    files = os.listdir(folder_path)
    
    for file in files:
        file_path = os.path.join(folder_path, file)
        if os.path.isfile(file_path) and "zip" not in file:
            upload_file_to_bucket(bucket, file_path)


def main(
    dataset_path: str,
    local_path: str,
    bucket_name: str | None
):
    print("Starting download...")
    file_path = download_dataset(dataset_path, local_path)
    print("Starting unzip...")
    unzip_dataset(file_path, local_path)
    print("Starting upload to MinIO...")
    upload_dataset_to_minio(bucket_name, local_path)
    print("Cleaning up...")
    shutil.rmtree(local_path)  # Deletes folder and all contents
    print("Done!")
