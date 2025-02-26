import requests  # type: ignore

from . import constants, utils

def validate_file(file_path: str, omop_version: str, delivery_date: str, gcs_path: str) -> None:
    utils.logger.info(f"Validating schema of {file_path} against OMOP v{omop_version}")
    response = requests.post(
        f"{constants.PROCESSOR_ENDPOINT}/validate_file",
        headers=utils.get_auth_header(),
        json={
            "file_path": file_path,
            "omop_version": omop_version,
            "delivery_date": delivery_date,
            "gcs_path": gcs_path
        }
    )
    response.raise_for_status()

def generate_delivery_report(report_data: dict) -> None:
    utils.logger.info(f"Generating final delivery report for {report_data['delivery_date']} delivery from {report_data['site']}")
    # This was already using POST, so no changes needed
    response = requests.post(
        f"{constants.PROCESSOR_ENDPOINT}/generate_delivery_report",
        headers=utils.get_auth_header(),
        json=report_data
    )
    response.raise_for_status()