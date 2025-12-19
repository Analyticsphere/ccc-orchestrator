import logging
import os
import subprocess
import sys
from datetime import datetime
from typing import Any, Dict, Optional, Tuple, Union

import requests  # type: ignore
import yaml  # type: ignore
from dependencies.ehr import constants
from google.cloud import storage  # type: ignore

"""
Set up a logging instance that will write to stdout (and therefore show up in Google Cloud logs)
"""
logging.basicConfig(
    level=logging.INFO,
    format='ehr-dag - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
# Create the logger at module level so its settings are applied throughout code base
logger = logging.getLogger(__name__)


def format_log_context(
    site: Optional[str] = None,
    delivery_date: Optional[str] = None,
    file: Optional[str] = None
) -> str:
    """
    Format contextual information for log messages.

    Creates a consistent, concise context prefix for log messages that includes
    site, delivery_date, and/or file information. This makes logs easily searchable
    and filterable in Cloud Logging.

    Args:
        site: Site name (e.g., 'synthea_54')
        delivery_date: Delivery date in YYYY-MM-DD format
        file: Filename, table name, or file path (extensions will be stripped)

    Returns:
        Formatted context string like "[site|delivery_date|file]" or subset.
        Returns empty string if no context provided.
    """
    parts = []
    if site:
        parts.append(site)
    if delivery_date:
        parts.append(delivery_date)
    if file:
        # Strip file extension for cleaner logging
        # Extract just the basename if it's a full path
        file_base = os.path.basename(file)
        # Remove extension (handles .csv, .parquet, etc.)
        file_without_ext, _ = os.path.splitext(file_base)
        # Only add if we have a meaningful name (not just an extension like ".csv")
        if file_without_ext:
            parts.append(file_without_ext)

    return f"[{'|'.join(parts)}] " if parts else ""


def extract_context_from_file_config(file_config_dict: dict) -> dict[str, Optional[str]]:
    """
    Extract site, delivery_date, and file context from a file_config_dict.

    Args:
        file_config_dict: Dictionary containing file configuration (from FileConfig.to_dict())

    Returns:
        Dict with keys: site, delivery_date, file (source_file or table_name)
    """
    return {
        'site': file_config_dict.get(constants.FileConfig.SITE.value),
        'delivery_date': file_config_dict.get(constants.FileConfig.DELIVERY_DATE.value),
        'file': (
            file_config_dict.get(constants.FileConfig.SOURCE_FILE.value) or
            file_config_dict.get(constants.FileConfig.TABLE_NAME.value)
        )
    }


def extract_context_from_site_tuple(site_to_process: tuple[str, str]) -> dict[str, Optional[str]]:
    """
    Extract site and delivery_date context from a site_to_process tuple.

    Args:
        site_to_process: Tuple of (site, delivery_date)

    Returns:
        Dict with keys: site, delivery_date, file (None)
    """
    site, delivery_date = site_to_process
    return {
        'site': site,
        'delivery_date': delivery_date,
        'file': None
    }

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
        raise Exception(f"Error getting authentication token: {e}") from e
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error checking service health: {e}") from e

def get_site_bucket(site: str) -> str:
    """
    Return the parent GCS bucket for a given site
    """
    return get_site_config_file()[constants.FileConfig.SITE.value][site][constants.FileConfig.GCS_BUCKET.value]

def get_site_config(site: str) -> dict:
    """
    Return the configuration dictionary for a specific site.
    
    Args:
        site: The site name
    
    Returns:
        Dictionary containing the site's configuration
    """
    return get_site_config_file()[constants.FileConfig.SITE.value][site]

def get_site_config_file() -> dict:
    """
    Return the site configuration YAML file as a dictionary
    """
    try:
        with open(constants.SITE_CONFIG_YML_PATH, 'r') as site_config_file:
            config = yaml.safe_load(site_config_file)

        return config
    except Exception as e:
        raise Exception(f"Unable to get site configuration file: {e}") from e

def get_site_list() -> list[str]:
    """
    Create list of all sites, using the site_config.yml file
    """
    config = get_site_config_file()
    sites = list(config[constants.FileConfig.SITE.value].keys()) 

    return sites

def get_most_recent_folder(site: str) -> str:
    """
    Find the most recent date-formatted folder in a GCS bucket.

    Args:
        site: Site name to find delivery dates for

    Returns:
        Most recent folder name in YYYY-MM-DD format

    Raises:
        Exception: If no date-formatted folders are found in the bucket
    """
    gcs_bucket = get_site_bucket(site)

    logger.info(f"[{site}] Searching for most recent delivery date in bucket {gcs_bucket}")

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

    if most_recent_folder is None:
        raise Exception(f"[{site}] No date-formatted folders found in bucket {gcs_bucket}")

    logger.info(f"[{site}] Found most recent delivery date: {most_recent_folder}")
    return most_recent_folder

def get_run_id(airflow_context) -> str:
    # Retrieve run_id from current Airflow context
    run_id = airflow_context['dag_run'].run_id if airflow_context.get('dag_run') else "unknown"

    return run_id

def get_file_path(file_config: dict) -> str:
    file_path = (
        f"{file_config[constants.FileConfig.GCS_BUCKET.value]}/"
        f"{file_config[constants.FileConfig.DELIVERY_DATE.value]}/"
        f"{file_config[constants.FileConfig.SOURCE_FILE.value]}"
    )

    return file_path

def make_api_call(
                 url: Optional[str],
                 endpoint: str,
                 method: str = "post",
                 params: Optional[Dict[str, str]] = None,
                 json_data: Optional[Dict[str, Any]] = None,
                 timeout: Optional[Union[float, Tuple[float, float]]] = None,
                 site: Optional[str] = None,
                 delivery_date: Optional[str] = None,
                 file: Optional[str] = None
                 ) -> Optional[Any]:
    """
    Makes an API call to the processor endpoint with standardized error handling.

    Args:
        url: Base URL of the API
        endpoint: Specific endpoint path
        method: HTTP method (get or post)
        params: Query parameters for GET requests
        json_data: JSON body for POST requests
        timeout: Request timeout
        site: Optional site name for logging context
        delivery_date: Optional delivery date for logging context
        file: Optional file/table name for logging context
    """
    # Import here to avoid circular dependency
    

    api_call_url = f"{url}/{endpoint}"
    log_ctx = format_log_context(site=site, delivery_date=delivery_date, file=file)

    # pipeline_log calls are made often and clutter the logs, don't display this message
    if endpoint != "pipeline_log":
        logger.info(f"{log_ctx}Making {method.upper()} request to {endpoint}")
    
    try:
        if method.lower() == "get":
            response = requests.get(
                api_call_url,
                headers=get_auth_header(),
                params=params,
                timeout=timeout
            )
        else:  # POST
            response = requests.post(
                api_call_url,
                headers=get_auth_header(),
                json=json_data,
                timeout=timeout
            )
        
        # Check if the request was successful (accept any 2xx response)
        if not (200 <= response.status_code < 300):
            error_message = f"{log_ctx}API Error {response.status_code} from {endpoint}: {response.text}"
            raise Exception(error_message)
        else:
            logger.info(f"{log_ctx}API call to {endpoint} successful: {response.text}")
        
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
        logger.warning(f"{log_ctx}Response from {endpoint} was not valid JSON: {response.text}")
        return response.text
    except subprocess.CalledProcessError as e:
        error_msg = f"{log_ctx}Error getting authentication token for {endpoint}: {str(e)}"
        raise Exception(error_msg) from e
    except requests.exceptions.RequestException as e:
        error_msg = f"{log_ctx}Network error when calling {endpoint}: {str(e)}"
        raise Exception(error_msg) from e