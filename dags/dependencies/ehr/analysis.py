from airflow.providers.google.cloud.operators.cloud_run import \
    CloudRunExecuteJobOperator  # type: ignore
from dependencies.ehr import constants, utils


def run_dqd_job(
    project_id: str,
    dataset_id: str,
    analytics_dataset_id: str,
    gcs_artifact_path: str,
    cdm_version: str,
    cdm_source_name: str,
    context,
    site: str = None,
    delivery_date: str = None
) -> None:
    """
    Execute DQD (Data Quality Dashboard) .

    DQD runs can take 2+ hours, exceeding the 1-hour Cloud Run service timeout.
    This function triggers a Cloud Run Job that can run up to 24 hours.

    Args:
        project_id: Google Cloud project ID
        dataset_id: BigQuery dataset ID
        analytics_dataset_id: BigQuery analytics dataset ID
        gcs_artifact_path: GCS path for artifacts
        cdm_version: OMOP CDM version (e.g., "5.4")
        cdm_source_name: Human-friendly name for the CDM source
        context: Airflow task context
        site: Optional site identifier for logging context
        delivery_date: Optional delivery date for logging context

    Raises:
        Exception: If Cloud Run Job fails
    """
    log_ctx = utils.format_log_context(site=site, delivery_date=delivery_date)
    utils.logger.info(f"{log_ctx}Executing DQD (Data Quality Dashboard) Cloud Run Job for: {project_id}.{dataset_id}")

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
                    {'name': 'ANALYTICS_DATASET_ID', 'value': analytics_dataset_id},
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
    utils.logger.info(f"{log_ctx}DQD Cloud Run Job completed successfully for: {project_id}.{dataset_id}")


def run_achilles_job(
    project_id: str,
    dataset_id: str,
    analytics_dataset_id: str,
    gcs_artifact_path: str,
    cdm_version: str,
    cdm_source_name: str,
    context,
    site: str = None,
    delivery_date: str = None
) -> None:
    """
    Execute Achilles analyses .

    Achilles runs can take 2+ hours, exceeding the 1-hour Cloud Run service timeout.
    This function triggers a Cloud Run Job that can run up to 24 hours.
    After Achilles completes, it executes a SQL script against BigQuery.

    Args:
        project_id: Google Cloud project ID
        dataset_id: BigQuery CDM dataset ID (where Achilles reads CDM data from)
        analytics_dataset_id: BigQuery analytics dataset ID (where Achilles writes results to)
        gcs_artifact_path: GCS path for artifacts
        cdm_version: OMOP CDM version (e.g., "5.4")
        cdm_source_name: Human-friendly name for the CDM source
        context: Airflow task context
        site: Optional site identifier for logging context
        delivery_date: Optional delivery date for logging context

    Raises:
        Exception: If Cloud Run Job fails
    """
    log_ctx = utils.format_log_context(site=site, delivery_date=delivery_date)
    utils.logger.info(f"{log_ctx}Executing Achilles Cloud Run Job - CDM dataset: {project_id}.{dataset_id}, Results dataset: {project_id}.{analytics_dataset_id}")

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
                    {'name': 'ANALYTICS_DATASET_ID', 'value': analytics_dataset_id},
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
    utils.logger.info(f"{log_ctx}Achilles Cloud Run Job completed successfully for: {project_id}.{dataset_id}")


def run_pass_job(
    project_id: str,
    dataset_id: str,
    gcs_artifact_path: str,
    context,
    site: str = None,
    delivery_date: str = None
) -> None:
    """
    Execute PASS (Profile of Analytic Suitability Score)

    PASS runs quality assessment across six dimensions to evaluate data fitness
    for research.

    Args:
        project_id: Google Cloud project ID
        dataset_id: BigQuery CDM dataset ID (where PASS reads CDM data from)
        gcs_artifact_path: GCS path for artifacts (e.g., "gs://bucket/delivery_date/artifacts/pass/")
        context: Airflow task context
        site: Optional site identifier for logging context
        delivery_date: Optional delivery date for logging context

    Raises:
        Exception: If Cloud Run Job fails
    """
    log_ctx = utils.format_log_context(site=site, delivery_date=delivery_date)
    utils.logger.info(f"{log_ctx}Executing PASS (Profile of Analytic Suitability Score) Cloud Run Job for: {project_id}.{dataset_id}")

    # Create and execute Cloud Run Job operator
    operator = CloudRunExecuteJobOperator(
        task_id=f'pass_job_{dataset_id}',
        project_id=project_id,
        region='us-central1',
        job_name='ccc-omop-analyzer-pass-job',
        overrides={
            'container_overrides': [{
                'env': [
                    {'name': 'PROJECT_ID', 'value': project_id},
                    {'name': 'CDM_DATASET_ID', 'value': dataset_id},
                    {'name': 'GCS_ARTIFACT_PATH', 'value': gcs_artifact_path}
                ]
            }]
        },
        deferrable=False  # Blocking execution - waits for job completion
    )

    # Execute the Cloud Run Job
    operator.execute(context=context)
    utils.logger.info(f"{log_ctx}PASS Cloud Run Job completed successfully for: {project_id}.{dataset_id}")


def create_atlas_results_tables(
    project_id: str,
    cdm_dataset_id: str,
    analytics_dataset_id: str,
    site: str = None,
    delivery_date: str = None
) -> None:
    """
    Create Atlas results tables in BigQuery by calling the OMOP analyzer API.

    This function calls the create_atlas_results_tables endpoint which executes
    SQL to create the necessary tables for storing Atlas/Achilles results.

    Args:
        project_id: Google Cloud project ID
        cdm_dataset_id: BigQuery CDM dataset ID
        analytics_dataset_id: BigQuery dataset ID where Atlas results tables will be created
        site: Optional site identifier for logging context
        delivery_date: Optional delivery date for logging context

    Raises:
        Exception: If API call fails
    """

    log_ctx = utils.format_log_context(site=site, delivery_date=delivery_date)
    utils.logger.info(f"{log_ctx}Creating Atlas results tables in: {project_id}.{analytics_dataset_id}")

    # Call the API endpoint
    response = utils.make_api_call(
        url=constants.OMOP_ANALYZER_ENDPOINT,
        endpoint="create_atlas_results_tables",
        method="post",
        json_data={
            "project_id": project_id,
            "cdm_dataset_id": cdm_dataset_id,
            "analytics_dataset_id": analytics_dataset_id
        },
        timeout=300,  # 5 minute timeout
        site=site,
        delivery_date=delivery_date
    )

    if response and response.get('status') == 'success':
        utils.logger.info(f"{log_ctx}Atlas results tables created successfully in: {project_id}.{analytics_dataset_id}")
    else:
        error_msg = f"{log_ctx}Failed to create Atlas results tables: {response}"
        raise Exception(error_msg)


def generate_delivery_report(
    gcs_bucket: str,
    delivery_date: str,
    site: str
) -> None:
    """
    Generate interactive HTML delivery report by calling the OMOP analyzer API.

    This function constructs the appropriate GCS paths for the delivery report CSV,
    DQD results CSV, PASS results directory, and output HTML file, then calls the
    generate_delivery_report endpoint to create a comprehensive visual report.

    Args:
        gcs_bucket: GCS bucket path (e.g., "synthea_cdm53")
        delivery_date: Delivery date string (e.g., "2025-01-01")
        site: Site identifier (e.g., "synthea_53")

    Raises:
        Exception: If API call fails
    """
    log_ctx = utils.format_log_context(site=site, delivery_date=delivery_date)
    utils.logger.info(f"{log_ctx}Generating OMOP delivery report")

    # Construct file paths
    delivery_report_csv = f"delivery_report_{site}_{delivery_date}.csv"
    delivery_report_path = f"gs://{gcs_bucket}/{delivery_date}/{constants.ArtifactPaths.REPORT.value}{delivery_report_csv}"
    dqd_results_path = f"gs://{gcs_bucket}/{delivery_date}/{constants.ArtifactPaths.DQD.value}dqdashboard_results.csv"
    pass_results_path = f"gs://{gcs_bucket}/{delivery_date}/{constants.ArtifactPaths.PASS_ANALYSIS.value}"
    output_gcs_path = f"gs://{gcs_bucket}/{delivery_date}/{constants.ArtifactPaths.REPORT.value}omop_delivery_report.html"

    utils.logger.info(f"{log_ctx}Input paths:")
    utils.logger.info(f"{log_ctx}  - Delivery report: {delivery_report_path}")
    utils.logger.info(f"{log_ctx}  - DQD results: {dqd_results_path}")
    utils.logger.info(f"{log_ctx}  - PASS results: {pass_results_path}")
    utils.logger.info(f"{log_ctx}Output path: {output_gcs_path}")

    # Call the API endpoint
    response = utils.make_api_call(
        url=constants.OMOP_ANALYZER_ENDPOINT,
        endpoint="generate_delivery_report",
        method="post",
        json_data={
            "delivery_report_path": delivery_report_path,
            "dqd_results_path": dqd_results_path,
            "output_gcs_path": output_gcs_path,
            "pass_results_path": pass_results_path
        },
        timeout=1800,  # 30 minute timeout
        site=site,
        delivery_date=delivery_date
    )

    if response and response.get('status') == 'success':
        utils.logger.info(f"{log_ctx}OMOP delivery report generated successfully!")
        utils.logger.info(f"{log_ctx}Report saved to: {output_gcs_path}")
    else:
        error_msg = f"{log_ctx}Failed to generate OMOP delivery report: {response}"
        raise Exception(error_msg)