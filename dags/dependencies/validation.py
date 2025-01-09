from . import utils
from . import constants
import requests

def validate_file(gcs_file_path: str, omop_version: str) -> None:
    utils.logger.info(f"Validating schema of {gcs_file_path} against OMOP v{omop_version}")
    response = requests.get(
        f"{constants.PROCESSOR_ENDPOINT}/validate_file?gcs_file_path={gcs_file_path}?omop_version={omop_version}",
        headers=utils.get_auth_header()
    )
    response.raise_for_status()
