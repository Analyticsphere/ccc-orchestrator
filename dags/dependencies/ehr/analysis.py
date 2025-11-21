from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator  # type: ignore
from dependencies.ehr import constants, utils


def run_dqd_job(
    project_id: str,
    dataset_id: str,
    gcs_artifact_path: str,
    cdm_version: str,
    cdm_source_name: str,
    context
) -> None:
    """
    Execute DQD via Cloud Run Job.

    DQD runs can take 2+ hours, exceeding the 1-hour Cloud Run service timeout.
    This function triggers a Cloud Run Job that can run up to 24 hours.

    Args:
        project_id: Google Cloud project ID
        dataset_id: BigQuery dataset ID
        gcs_artifact_path: GCS path for artifacts
        cdm_version: OMOP CDM version (e.g., "5.4")
        cdm_source_name: Human-friendly name for the CDM source
        context: Airflow task context

    Raises:
        Exception: If Cloud Run Job fails
    """
    utils.logger.info(f"Executing DQD Cloud Run Job for {project_id}.{dataset_id}")

    # Create and execute Cloud Run Job operator
    operator = CloudRunExecuteJobOperator(
        task_id=f'dqd_job_{dataset_id}',
        project_id=project_id,
        region='us-central1',
        job_name='ccc-omop-analyzer-dqd-job',
        overrides={
            'container_overrides': [{
                'env': [
                    {'name': 'PROJECT_ID', 'value': project_id},
                    {'name': 'CDM_DATASET_ID', 'value': dataset_id},
                    {'name': 'GCS_ARTIFACT_PATH', 'value': gcs_artifact_path},
                    {'name': 'CDM_VERSION', 'value': cdm_version},
                    {'name': 'CDM_SOURCE_NAME', 'value': cdm_source_name}
                ]
            }]
        },
        deferrable=False  # Blocking execution - waits for job completion
    )

    # Execute the Cloud Run Job
    operator.execute(context=context)
    utils.logger.info(f"DQD Cloud Run Job completed successfully for {project_id}.{dataset_id}")


def run_achilles_job(
    project_id: str,
    dataset_id: str,
    gcs_artifact_path: str,
    cdm_version: str,
    cdm_source_name: str,
    context
) -> None:
    """
    Execute Achilles analyses via Cloud Run Job.

    Achilles runs can take 2+ hours, exceeding the 1-hour Cloud Run service timeout.
    This function triggers a Cloud Run Job that can run up to 24 hours.
    After Achilles completes, it executes a SQL script against BigQuery.

    Args:
        project_id: Google Cloud project ID
        dataset_id: BigQuery dataset ID
        gcs_artifact_path: GCS path for artifacts
        cdm_version: OMOP CDM version (e.g., "5.4")
        cdm_source_name: Human-friendly name for the CDM source
        context: Airflow task context

    Raises:
        Exception: If Cloud Run Job fails
    """
    utils.logger.info(f"Executing Achilles Cloud Run Job for {project_id}.{dataset_id}")

    # Create and execute Cloud Run Job operator
    operator = CloudRunExecuteJobOperator(
        task_id=f'achilles_job_{dataset_id}',
        project_id=project_id,
        region='us-central1',
        job_name='ccc-omop-analyzer-achilles-job',
        overrides={
            'container_overrides': [{
                'env': [
                    {'name': 'PROJECT_ID', 'value': project_id},
                    {'name': 'CDM_DATASET_ID', 'value': dataset_id},
                    {'name': 'GCS_ARTIFACT_PATH', 'value': gcs_artifact_path},
                    {'name': 'CDM_VERSION', 'value': cdm_version},
                    {'name': 'CDM_SOURCE_NAME', 'value': cdm_source_name}
                ]
            }]
        },
        deferrable=False  # Blocking execution - waits for job completion
    )

    # Execute the Cloud Run Job
    operator.execute(context=context)
    utils.logger.info(f"Achilles Cloud Run Job completed successfully for {project_id}.{dataset_id}")


def create_atlas_results_tables(
    project_id: str,
    cdm_dataset_id: str,
    atlas_results_dataset_id: str
) -> None:
    """
    Create Atlas results tables in BigQuery by calling the OMOP analyzer API.

    This function calls the create_atlas_results_tables endpoint which executes
    SQL to create the necessary tables for storing Atlas/Achilles results.

    Args:
        project_id: Google Cloud project ID
        cdm_dataset_id: BigQuery CDM dataset ID
        atlas_results_dataset_id: BigQuery dataset ID where Atlas results tables will be created

    Raises:
        Exception: If API call fails
    """
    utils.logger.info(f"Creating Atlas results tables in {project_id}.{atlas_results_dataset_id}")

    # Call the API endpoint
    response = utils.make_api_call(
        url=constants.OMOP_ANALYZER_ENDPOINT,
        endpoint="create_atlas_results_tables",
        method="post",
        json_data={
            "project_id": project_id,
            "cdm_dataset_id": cdm_dataset_id,
            "atlas_results_dataset_id": atlas_results_dataset_id
        },
        timeout=300  # 5 minute timeout
    )

    if response and response.get('status') == 'success':
        utils.logger.info(f"Atlas results tables created successfully in {project_id}.{atlas_results_dataset_id}")
    else:
        error_msg = f"Failed to create Atlas results tables: {response}"
        utils.logger.error(error_msg)
        raise Exception(error_msg)