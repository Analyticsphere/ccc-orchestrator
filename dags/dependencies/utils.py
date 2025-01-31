from . import constants
import requests
import subprocess
import logging
import sys
import yaml

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
        sys.exit(1)

def get_auth_header() -> str:
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
        # Get the token
        token = get_gcloud_token()
        
        # Make the authenticated request
        response = requests.get(
            f"{base_url}/heartbeat",
            headers=get_auth_header()
        )
        response.raise_for_status()
        return response.json()
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Error getting authentication token: {e}")
        sys.exit(1)
    except requests.exceptions.RequestException as e:
        logger.error(f"Error checking service health: {e}")
        sys.exit(1)

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
        sys.exit(1)

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

def get_date_prefix(file_name: str) -> str:
    """
    Extracts date prefix from files in format YYYY-MM-DD/file_name.csv
    Returns the YYYY-MM-DD portion
    """
    return file_name.split('/')[0]