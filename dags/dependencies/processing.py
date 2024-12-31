from . import utils
from . import constants
import logging
import sys
import requests
import subprocess
from google.cloud import storage
from datetime import datetime

def get_file_list(site: str) -> list[str]:
    """
    Get a list of files from a site's latest delivery
    """
    try:
        gcs_bucket = utils.get_site_bucket(site)
        delivery_date = get_most_recent_folder(site)

        full_path = f"{gcs_bucket}/{delivery_date}"
        create_artifact_buckets(full_path)

        utils.logger.info(f"Getting files for {delivery_date} delivery from {site}")

        # Make the authenticated request
        response = requests.get(
            f"{constants.PROCESSOR_ENDPOINT}/get_file_list?bucket={gcs_bucket}&folder={delivery_date}",
            headers=utils.get_auth_header()
        )
        response.raise_for_status()

        # File list will have strings like YYYY-MM-DD/file_name.extension
        # Using date part of file path for downstream use
        filenames = response.json()['file_list']

        return filenames
        
    except subprocess.CalledProcessError as e:
        logging.error(f"Error getting authentication token: {e}")
        sys.exit(1)
    except requests.exceptions.RequestException as e:
        logging.error(f"Error getting file list: {e}")
        sys.exit(1)
    return []

def get_most_recent_folder(site: str) -> str:
    """
    Find the most recent date-formatted folder in a GCS bucket.
    """
    gcs_bucket = utils.get_site_bucket(site)

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(gcs_bucket)

    # Get all blobs
    blobs = list(bucket.list_blobs())
    
    # Extract unique top-level folder names
    top_level_folders = set()
    for blob in blobs:
        # Split the path and take the first segment
        parts = blob.name.split('/')
        if parts and parts[0]:  # Make sure we have a non-empty first segment
            top_level_folders.add(parts[0])
    
    most_recent_date = None
    most_recent_folder = None
    
    # Check each folder
    for folder_name in top_level_folders:
        try:
            # Try to parse the folder name as a date
            folder_date = datetime.strptime(folder_name, '%Y-%m-%d')
            
            # Update most recent if this is the first or a more recent date
            if most_recent_date is None or folder_date > most_recent_date:
                most_recent_date = folder_date
                most_recent_folder = folder_name
                
        except ValueError:
            # Skip folders that don't match our date format
            continue

    return most_recent_folder

def create_artifact_buckets(parent_bucket: str) -> None:
    utils.logger.info(f"Creating artifact bucket in {parent_bucket}")
    reponse = requests.get(
        f"{constants.PROCESSOR_ENDPOINT}/create_artifact_buckets?parent_bucket={parent_bucket}",
        headers=utils.get_auth_header()
    )
    reponse.raise_for_status()

def convert_to_parquet(file_path: str) -> None:
    utils.logger.info(f"Converting file gs://{file_path} to Parquet")
    response = requests.get(
        f"{constants.PROCESSOR_ENDPOINT}/convert_to_parquet?file_path={file_path}",
        headers=utils.get_auth_header()
    )
    response.raise_for_status()