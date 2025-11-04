from dependencies.ehr import utils
from dependencies.ehr import constants

def run_dqd(project_id: str, dataset_id: str, gcs_artifact_path: str, cdm_version: str, cdm_source_name: str) -> None:
    utils.logger.info(f"Running dqd_checks for {project_id}.{dataset_id}")
    utils.make_api_call(
        url=constants.OMOP_ANALYZER_ENDPOINT,
        endpoint="run_dqd",
        json_data={
            "project_id": project_id,
            "dataset_id": dataset_id,
            "gcs_artifact_path": gcs_artifact_path,
            "cdm_version": cdm_version,
            "cdm_source_name": cdm_source_name               
        }
    )

def run_achilles(project_id: str, dataset_id: str, gcs_artifact_path: str) -> None:
    utils.logger.info(f"Running Achilles for {project_id}.{dataset_id}")
    utils.make_api_call(
        url = constants.OMOP_ANALYZER_ENDPOINT,
        endpoint="run_achilles",
        json_data={
            "project_id": project_id,
            "dataset_id": dataset_id,  
            "gcs_artifact_path": gcs_artifact_path          
        }
    )