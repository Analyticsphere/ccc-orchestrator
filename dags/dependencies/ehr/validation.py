from dependencies.ehr import utils


def validate_file(file_path: str, omop_version: str, delivery_date: str, gcs_path: str) -> None:
    utils.logger.info(f"Validating schema of {file_path} against OMOP v{omop_version}")
    
    utils.make_api_call(
        endpoint="validate_file",
        json_data={
            "file_path": file_path,
            "omop_version": omop_version,
            "delivery_date": delivery_date,
            "gcs_path": gcs_path
        }
    )

def generate_delivery_report(report_data: dict) -> None:
    utils.logger.info(f"Generating final delivery report for {report_data['delivery_date']} delivery from {report_data['site']}")
    
    utils.make_api_call(
        endpoint="generate_delivery_report",
        json_data=report_data
    )