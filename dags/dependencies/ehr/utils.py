import logging
import subprocess
import sys
from datetime import datetime
from typing import Any, Dict, Optional

import requests  # type: ignore
import yaml  # type: ignore
from google.cloud import storage  # type: ignore

from . import constants, file_config

"""
Set up a logging instance that will write to stdout (and therefore show up in Google Cloud logs)
"""
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
# Create the logger at module level so its settings are applied throughout code base
logger = logging.getLogger(__name__)

def get_gcloud_token() -> str:
    """
    Get identity token from gcloud CLI
    """
    try:
        # subprocess runs a system command; using it to obtain and then return token
        token = subprocess.check_output(
            ['gcloud', 'auth', 'print-identity-token'],
            universal_newlines=True
        ).strip()
        return token
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to get gcloud token: {e}")
        raise Exception(f"Failed to get gcloud token: {e}") from e

def get_auth_header() -> dict[str, str]:
    """
    Set up headers with bearer token
    """
    headers = {'Authorization': f'Bearer {get_gcloud_token()}'}
    return headers

def check_service_health(base_url: str) -> dict:
    """
    Call the heartbeat endpoint to check service health.
    """
    logger.info("Trying to get API health status")
    try:        
        # Make authenticated request
        response = requests.get(
            f"{base_url}/heartbeat",
            headers=get_auth_header()
        )
        response.raise_for_status()
        return response.json()
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Error getting authentication token: {e}")
        raise Exception(f"Error getting authentication token: {e}") from e
    except requests.exceptions.RequestException as e:
        logger.error(f"Error checking service health: {e}")
        raise Exception(f"Error checking service health: {e}") from e

def get_site_bucket(site: str) -> str:
    """
    Return the parent GCS bucket for a given site
    """
    return get_site_config_file()[constants.FileConfig.SITE.value][site][constants.FileConfig.GCS_PATH.value]

def get_site_config_file() -> dict:
    """
    Return the site configuration YAML file as a dictionary
    """
    try:
        with open(constants.SITE_CONFIG_YML_PATH, 'r') as site_config_file:
            config = yaml.safe_load(site_config_file)

        return config
    except Exception as e:
        logger.error(f"Unable to get site configuration file: {e}")
        raise Exception(f"Unable to get site configuration file: {e}") from e

def get_site_list() -> list[str]:
    """
    Create list of all sites, using the site_config.yml file
    """
    config = get_site_config_file()
    sites = list(config[constants.FileConfig.SITE.value].keys()) 

    return sites

def remove_date_prefix(file_name: str) -> str:
    """
    Removes date prefix from files pulled from EHR OMOP GCS buckets in format YYYY-MM-DD/file_name.csv
    """
    return file_name.split('/')[-1]

# def get_date_prefix(file_name: str) -> str:
#     """
#     Extracts date prefix from files in format YYYY-MM-DD/file_name.csv
#     Returns the YYYY-MM-DD portion
#     """
#     return file_name.split('/')[0]

def get_most_recent_folder(site: str) -> str:
    """
    Find the most recent date-formatted folder in a GCS bucket.
    """
    gcs_bucket = get_site_bucket(site)

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

def get_run_id(airflow_context) -> str:
    # Retrieve run_id from current Airflow context
    run_id = airflow_context['dag_run'].run_id if airflow_context.get('dag_run') else "unknown"

    return run_id

def get_file_path(file_config: file_config.FileConfig) -> str:
    file_path = (
        f"{file_config[constants.FileConfig.GCS_PATH.value]}/"
        f"{file_config[constants.FileConfig.DELIVERY_DATE.value]}/"
        f"{file_config[constants.FileConfig.FILE_NAME.value]}"
    )

    return file_path

def make_api_call(endpoint: str, method: str = "post", 
                 params: Optional[Dict[str, str]] = None, 
                 json_data: Optional[Dict[str, Any]] = None, 
                 timeout: Optional[tuple] = None) -> Optional[Any]:
    """
    Makes an API call to the processor endpoint with standardized error handling.
    """
    url = f"{constants.PROCESSOR_ENDPOINT}/{endpoint}"

    # pipeline_log calls are made often and clutter the logs, don't display this message
    if endpoint != "pipeline_log":
        logger.info(f"Making {method.upper()} request to {url}")
    
    try:
        if method.lower() == "get":
            response = requests.get(
                url,
                headers=get_auth_header(),
                params=params,
                timeout=timeout
            )
        else:  # POST
            response = requests.post(
                url,
                headers=get_auth_header(),
                json=json_data,
                timeout=timeout
            )
        
        # Check if the request was successful
        if response.status_code != 200:
            error_message = f"API Error {response.status_code} from {endpoint}: {response.text}"
            logger.error(error_message)
            raise Exception(error_message)
        
        # Try to parse as JSON, but handle non-JSON responses
        if response.content:
            try:
                return response.json()
            except ValueError:
                # Not JSON, return text instead
                return response.text
        return None
        
    except requests.exceptions.JSONDecodeError as e:
        # This shouldn't normally be reached with the try/except above,
        # but keeping it as a fallback
        logger.warning(f"Response from {endpoint} was not valid JSON: {response.text}")
        return response.text
    except subprocess.CalledProcessError as e:
        error_msg = f"Error getting authentication token for {endpoint}: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg) from e
    except requests.exceptions.RequestException as e:
        error_msg = f"Network error when calling {endpoint}: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg) from e