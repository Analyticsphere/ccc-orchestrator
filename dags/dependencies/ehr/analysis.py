import time
from dependencies.ehr import utils
from dependencies.ehr import constants

def run_dqd(
    project_id: str,
    dataset_id: str,
    gcs_artifact_path: str,
    cdm_version: str,
    cdm_source_name: str
) -> dict:
    """
    Run DQD checks synchronously.

    This function calls the /run_dqd endpoint which blocks until completion (up to 3 hours).

    Args:
        project_id: Google Cloud project ID
        dataset_id: BigQuery dataset ID
        gcs_artifact_path: GCS path for artifacts
        cdm_version: OMOP CDM version (e.g., "5.4")
        cdm_source_name: Human-friendly name for the CDM source

    Returns:
        dict: Job result information

    Raises:
        Exception: If job fails
    """
    utils.logger.info(f"Starting synchronous DQD for {project_id}.{dataset_id}")

    # Call the synchronous endpoint - it will block until completion
    response = utils.make_api_call(
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

    # Check status in response
    status = response.get("status")

    if status == "success":
        execution_time = response.get("execution_time_seconds", "unknown")
        utils.logger.info(f"DQD completed successfully in {execution_time} seconds")
        return response
    else:
        error_msg = response.get("details", response.get("message", "Unknown error"))
        utils.logger.error(f"DQD failed: {error_msg}")
        raise Exception(f"DQD failed: {error_msg}")

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