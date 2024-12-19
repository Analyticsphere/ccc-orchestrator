from . import utils
from . import constants
import logging
import sys
import requests
import subprocess
import yaml

def get_file_list(site: str) -> list[str]:


    logging.info("Trying to get list of files")
    try:
        # Get YAML data
        with open('/home/airflow/gcs/dags/config/site_config.yml', 'r') as site_config_file:
            config = yaml.safe_load(site_config_file)
        
        gcs_bucket = config['site'][site]['gcs_path']
        delivery_date = '2024-11-25'

        # Get the token
        token = utils.get_gcloud_token()
        
        # Set up headers with bearer token
        headers = {
            'Authorization': f'Bearer {token}'
        }

        # Make the authenticated request
        response = requests.get(
            f"{constants.PROCESSOR_ENDPOINT}/get_file_list?bucket={gcs_bucket}&folder={delivery_date}",
            headers=headers
        )
        response.raise_for_status()
        return response.json()
        
    except subprocess.CalledProcessError as e:
        logging.error(f"Error getting authentication token: {e}")
        sys.exit(1)
    except requests.exceptions.RequestException as e:
        logging.error(f"Error getting file list: {e}")
        sys.exit(1)
    return []