from . import utils
from . import constants
import requests # type: ignore
 
def validate_file(file_path: str, omop_version: str, delivery_date: str, gcs_path: str) -> None:
    utils.logger.info(f"Validating schema of {file_path} against OMOP v{omop_version}")
    response = requests.get(
        f"{constants.PROCESSOR_ENDPOINT}/validate_file?file_path={file_path}&omop_version={omop_version}&delivery_date={delivery_date}&gcs_path={gcs_path}",
        headers=utils.get_auth_header()
    )
    response.raise_for_status()
