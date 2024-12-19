import requests
import subprocess
import logging
import sys

def get_gcloud_token() -> str:
    """
    Get identity token from gcloud CLI.
    """
    logging.info("Getting GCloud token")
    try:
        # subprocess runs a system command; using it to obtain and then return token
        token = subprocess.check_output(
            ['gcloud', 'auth', 'print-identity-token'],
            universal_newlines=True
        ).strip()
        return token
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to get gcloud token: {e}")
        sys.exit(1)
    
def check_service_health(base_url):
    """
    Call the heartbeat endpoint to check service health.
    """
    logging.info("Trying to get API health status")
    try:
        # Get the token
        token = get_gcloud_token()
        
        # Set up headers with bearer token
        headers = {
            'Authorization': f'Bearer {token}'
        }
        
        # Make the authenticated request
        response = requests.get(
            f"{base_url}/heartbeat",
            headers=headers
        )
        response.raise_for_status()
        return response.json()
        
    except subprocess.CalledProcessError as e:
        logging.error(f"Error getting authentication token: {e}")
        sys.exit(1)
    except requests.exceptions.RequestException as e:
        logging.error(f"Error checking service health: {e}")
        sys.exit(1)
