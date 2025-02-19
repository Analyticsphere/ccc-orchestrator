import logging
import subprocess
import sys

import requests  # type: ignore

from . import constants, utils

def get_file_list(site: str, delivery_date: str, file_format: str) -> list[str]:
    """
    Get a list of files from a site's latest delivery
    """
    try:
        gcs_bucket = utils.get_site_bucket(site)

        full_path = f"{gcs_bucket}/{delivery_date}"
        create_artifact_buckets(full_path)

        utils.logger.info(f"Getting files for {delivery_date} delivery from {site}")

        # Make the authenticated request
        response = requests.get(
            f"{constants.PROCESSOR_ENDPOINT}/get_file_list?bucket={gcs_bucket}&folder={delivery_date}&file_format={file_format}",
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

def create_artifact_buckets(parent_bucket: str) -> None:
    utils.logger.info(f"Creating artifact bucket in {parent_bucket}")
    reponse = requests.get(
        f"{constants.PROCESSOR_ENDPOINT}/create_artifact_buckets?parent_bucket={parent_bucket}",
        headers=utils.get_auth_header()
    )
    reponse.raise_for_status()

def process_file(file_type: str, file_path: str) -> None:
    utils.logger.info(f"Processing incoming {file_type} file gs://{file_path}")
    response = requests.get(
        f"{constants.PROCESSOR_ENDPOINT}/convert_to_parquet?file_type={file_type}&file_path={file_path}",
        headers=utils.get_auth_header(),
        timeout=(10, 600)
    )
    
    response.raise_for_status()

def normalize_parquet_file(file_path: str, cdm_version: str) -> None:
    utils.logger.info(f"Normalizing Parquet file gs://{file_path}")
    response = requests.get(
        f"{constants.PROCESSOR_ENDPOINT}/normalize_parquet?file_path={file_path}&omop_version={cdm_version}",
        headers=utils.get_auth_header()
    )
    response.raise_for_status()

def upgrade_cdm(file_path: str, cdm_version: str, target_cdm_version: str) -> None:
    utils.logger.info(f"Upgrading CDM version {cdm_version} of file gs://{file_path} to {target_cdm_version}")
    response = requests.get(
        f"{constants.PROCESSOR_ENDPOINT}/upgrade_cdm?file_path={file_path}&omop_version={cdm_version}&target_omop_version={target_cdm_version}",
        headers=utils.get_auth_header()
    )
    response.raise_for_status()