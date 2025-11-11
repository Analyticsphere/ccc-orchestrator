import time
from dependencies.ehr import utils
from dependencies.ehr import constants

def run_dqd(
    project_id: str,
    dataset_id: str,
    gcs_artifact_path: str,
    cdm_version: str,
    cdm_source_name: str,
    poll_interval: int = 120,
    max_wait_minutes: int = 180
) -> dict:
    """
    Run DQD checks with async job pattern.

    This function:
    1. Starts a DQD job and gets a job_id
    2. Polls the status endpoint until completion
    3. Returns results or raises exception on failure

    Args:
        project_id: Google Cloud project ID
        dataset_id: BigQuery dataset ID
        gcs_artifact_path: GCS path for artifacts
        cdm_version: OMOP CDM version (e.g., "5.4")
        cdm_source_name: Human-friendly name for the CDM source
        poll_interval: Seconds to wait between status checks (default: 120)
        max_wait_minutes: Maximum minutes to wait for job completion (default: 180)

    Returns:
        dict: Job result information

    Raises:
        Exception: If job fails or times out
    """
    utils.logger.info(f"Starting DQD job for {project_id}.{dataset_id}")

    # Step 1: Start the DQD job
    start_response = utils.make_api_call(
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

    job_id = start_response.get("job_id")
    returned_gcs_path = start_response.get("gcs_artifact_path")

    # Handle case where job_id might be returned as a list
    if isinstance(job_id, list):
        job_id = job_id[0] if job_id else None

    # Handle case where gcs_artifact_path might be returned as a list
    if isinstance(returned_gcs_path, list):
        returned_gcs_path = returned_gcs_path[0] if returned_gcs_path else None

    if not job_id:
        raise Exception("No job_id returned from /run_dqd endpoint")

    # Use the returned gcs_artifact_path for status checks (in case it was modified by API)
    # Fallback to the original gcs_artifact_path if not returned or empty
    gcs_path_for_status = returned_gcs_path if returned_gcs_path else gcs_artifact_path

    # Validate we have a path for status checks
    if not gcs_path_for_status:
        raise Exception("No gcs_artifact_path available for status checks")

    utils.logger.info(f"DQD job started with job_id: {job_id}, gcs_path: {gcs_path_for_status}")

    # Step 2: Poll for completion
    start_time = time.time()
    max_wait_seconds = max_wait_minutes * 60
    poll_count = 0

    while True:
        # Check if we've exceeded max wait time
        elapsed = time.time() - start_time
        if elapsed > max_wait_seconds:
            error_msg = f"DQD job {job_id} timed out after {max_wait_minutes} minutes"
            utils.logger.error(error_msg)
            raise Exception(error_msg)

        # Poll status
        poll_count += 1
        utils.logger.info(f"Polling DQD job {job_id} status (attempt {poll_count}, elapsed: {int(elapsed)}s)")
        utils.logger.info(f"Status check params: gcs_artifact_path={gcs_path_for_status}")

        status_response = utils.make_api_call(
            url=constants.OMOP_ANALYZER_ENDPOINT,
            endpoint=f"status/{job_id}",
            method="get",
            params={"gcs_artifact_path": gcs_path_for_status}
        )

        status = status_response.get("status")
        utils.logger.info(f"DQD job {job_id} status: {status}")

        if status == "completed":
            utils.logger.info(f"DQD job {job_id} completed successfully after {int(elapsed)}s")
            return status_response
        elif status == "failed":
            error_msg = status_response.get("error", "Unknown error")
            utils.logger.error(f"DQD job {job_id} failed: {error_msg}")
            raise Exception(f"DQD job {job_id} failed: {error_msg}")
        elif status in ["pending", "running"]:
            # Still processing, wait and poll again
            utils.logger.info(f"DQD job {job_id} still {status}, waiting {poll_interval}s before next poll")
            time.sleep(poll_interval)
        else:
            error_msg = f"Unknown job status for {job_id}: {status}"
            utils.logger.error(error_msg)
            raise Exception(error_msg)

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