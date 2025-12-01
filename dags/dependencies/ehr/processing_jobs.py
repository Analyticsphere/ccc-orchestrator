"""
Cloud Run Jobs execution functions for EHR processing pipeline.

This module provides functions to execute long-running file processing operations
as Google Cloud Run Jobs instead of HTTP API calls. Cloud Run Jobs can run up to
24 hours (vs 1 hour limit for Cloud Run Services), making them suitable for
large file processing tasks.

Jobs executed by this module:
    - process_file: Convert CSV/CSV.GZ to Parquet
    - normalize_parquet: Standardize data types and formats
    - upgrade_cdm: Upgrade CDM versions (e.g., 5.3 to 5.4)
    - harmonize_vocab: 8-step vocabulary harmonization process
    - generate_derived_tables: Generate observation_period, condition_era, drug_era
"""

import json

from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator

from dependencies.ehr import constants, utils
from dependencies.ehr.storage_backend import storage


def run_process_file_job(
    file_type: str,
    gcs_file_path: str,
    project_id: str,
    context
) -> None:
    """
    Execute file processing as a Cloud Run Job (CSV/CSV.GZ to Parquet).

    Args:
        file_type: Type of OMOP table (e.g., 'person', 'condition_occurrence')
        gcs_file_path: Full GCS path to the file (gs://bucket/path/file.csv)
        project_id: GCP project ID
        context: Airflow task context
    """
    utils.logger.info(f"Executing process_file job for {file_type}: {gcs_file_path}")

    # Sanitize file_type for task_id (dots are not allowed in Airflow task IDs)
    sanitized_file_type = file_type.replace('.', '_')

    operator = CloudRunExecuteJobOperator(
        task_id=f'process_file_job_{sanitized_file_type}',
        project_id=project_id,
        region='us-central1',
        job_name='ccc-omop-file-processor-process-file-job',
        overrides={
            'container_overrides': [{
                'env': [
                    {'name': 'FILE_TYPE', 'value': file_type},
                    {'name': 'FILE_PATH', 'value': gcs_file_path}
                ]
            }]
        },
        deferrable=False  # Blocking execution
    )

    operator.execute(context=context)


def run_normalize_parquet_job(
    file_path: str,
    cdm_version: str,
    date_format: str,
    datetime_format: str,
    project_id: str,
    context
) -> None:
    """
    Execute Parquet normalization as a Cloud Run Job.

    Args:
        file_path: Original file path (will be converted to parquet artifact path)
        cdm_version: OMOP CDM version (e.g., '5.4')
        date_format: Date format string (e.g., '%Y-%m-%d')
        datetime_format: Datetime format string (e.g., '%Y-%m-%d %H:%M:%S')
        project_id: GCP project ID
        context: Airflow task context
    """
    utils.logger.info(f"Executing normalize_parquet job for: {file_path}")

    operator = CloudRunExecuteJobOperator(
        task_id=f'normalize_parquet_job',
        project_id=project_id,
        region='us-central1',
        job_name='ccc-omop-file-processor-normalize-parquet-job',
        overrides={
            'container_overrides': [{
                'env': [
                    {'name': 'FILE_PATH', 'value': file_path},
                    {'name': 'OMOP_VERSION', 'value': cdm_version},
                    {'name': 'DATE_FORMAT', 'value': date_format},
                    {'name': 'DATETIME_FORMAT', 'value': datetime_format}
                ]
            }]
        },
        deferrable=False
    )

    operator.execute(context=context)


def run_upgrade_cdm_job(
    file_path: str,
    cdm_version: str,
    target_cdm_version: str,
    project_id: str,
    context
) -> None:
    """
    Execute CDM upgrade as a Cloud Run Job.

    Args:
        file_path: GCS path to the file to upgrade
        cdm_version: Current OMOP CDM version
        target_cdm_version: Target OMOP CDM version
        project_id: GCP project ID
        context: Airflow task context
    """
    utils.logger.info(f"Executing upgrade_cdm job: {file_path} from {cdm_version} to {target_cdm_version}")

    operator = CloudRunExecuteJobOperator(
        task_id=f'upgrade_cdm_job',
        project_id=project_id,
        region='us-central1',
        job_name='ccc-omop-file-processor-upgrade-cdm-job',
        overrides={
            'container_overrides': [{
                'env': [
                    {'name': 'FILE_PATH', 'value': file_path},
                    {'name': 'OMOP_VERSION', 'value': cdm_version},
                    {'name': 'TARGET_OMOP_VERSION', 'value': target_cdm_version}
                ]
            }]
        },
        deferrable=False
    )

    operator.execute(context=context)


def run_harmonize_vocab_job(
    file_path: str,
    site: str,
    project_id: str,
    dataset_id: str,
    step: str,
    context,
    output_gcs_path: str = ""
) -> None:
    """
    Execute vocabulary harmonization step as a Cloud Run Job.

    Args:
        file_path: GCS path to the file to harmonize
        site: Site identifier
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        step: Harmonization step name (source_target, target_remap, target_replacement,
              domain_check, omop_etl, consolidate_etl, discover_tables_for_dedup, deduplicate_single_table)
        context: Airflow task context
        output_gcs_path: Optional GCS path to write results (for discover_tables step)
    """
    utils.logger.info(f"Executing harmonize_vocab job - Step: {step}, File: {file_path}")

    env_vars = [
        {'name': 'FILE_PATH', 'value': file_path},
        {'name': 'VOCAB_VERSION', 'value': constants.OMOP_TARGET_VOCAB_VERSION},
        {'name': 'OMOP_VERSION', 'value': constants.OMOP_TARGET_CDM_VERSION},
        {'name': 'SITE', 'value': site},
        {'name': 'PROJECT_ID', 'value': project_id},
        {'name': 'DATASET_ID', 'value': dataset_id},
        {'name': 'STEP', 'value': step}
    ]

    # Add optional output path for discover_tables step
    if output_gcs_path:
        env_vars.append({'name': 'OUTPUT_GCS_PATH', 'value': output_gcs_path})

    operator = CloudRunExecuteJobOperator(
        task_id=f'harmonize_vocab_job_{step}',
        project_id=project_id,
        region='us-central1',
        job_name='ccc-omop-file-processor-harmonize-vocab-job',
        overrides={
            'container_overrides': [{
                'env': env_vars
            }]
        },
        deferrable=False
    )

    operator.execute(context=context)


def run_discover_tables_job(
    file_path: str,
    site: str,
    project_id: str,
    dataset_id: str,
    gcs_bucket: str,
    delivery_date: str,
    context
) -> list[dict]:
    """
    Execute discover_tables step as a Cloud Run Job and retrieve results from GCS.

    This is a special case of harmonize_vocab that returns data (list of table configs).
    The job writes results to GCS, and this function reads them back.

    Args:
        file_path: Dummy file path (for API compatibility)
        site: Site identifier
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        gcs_bucket: GCS bucket for output
        delivery_date: Delivery date
        context: Airflow task context

    Returns:
        List of table configuration dictionaries for parallel processing
    """
    # Construct output path for results
    output_gcs_path = f"{gcs_bucket}/{delivery_date}/artifacts/temp/table_configs_{site}.json"

    utils.logger.info(f"Discovering tables for deduplication for site {site}")
    utils.logger.info(f"Results will be written to: {storage.get_uri(output_gcs_path)}")

    # Execute the job
    run_harmonize_vocab_job(
        file_path=file_path,
        site=site,
        project_id=project_id,
        dataset_id=dataset_id,
        step=constants.DISCOVER_TABLES_FOR_DEDUP,
        context=context,
        output_gcs_path=output_gcs_path
    )

    # Read results from GCS
    try:
        from google.cloud import storage as gcs_storage

        # Parse GCS path
        gcs_path = storage.strip_scheme(output_gcs_path)

        parts = gcs_path.split('/', 1)
        bucket_name = parts[0]
        blob_path = parts[1] if len(parts) > 1 else ''

        # Download and parse JSON
        storage_client = gcs_storage.Client(project=project_id)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        table_configs_json = blob.download_as_text()
        table_configs = json.loads(table_configs_json)

        utils.logger.info(f"Discovered {len(table_configs)} table(s) for deduplication")
        return table_configs

    except Exception as e:
        utils.logger.error(f"Failed to read table configs from GCS: {e}")
        # Return empty list if file doesn't exist or can't be read
        return []


def run_deduplicate_table_job(
    table_config: dict,
    context
) -> None:
    """
    Execute table deduplication as a Cloud Run Job.

    Args:
        table_config: Dictionary containing table configuration with keys:
            - table_name: Name of the table
            - file_path: Path to the consolidated parquet file
            - site: Site identifier
            - project_id: Google Cloud project ID
            - dataset_id: BigQuery dataset ID
            - cdm_version: CDM version
        context: Airflow task context
    """
    table_name = table_config.get('table_name', 'unknown')
    site = table_config.get('site', 'unknown')
    project_id = table_config['project_id']

    utils.logger.info(f"Executing deduplicate_table job for {table_name} from site {site}")

    # Pass the table config as JSON string in the file_path parameter
    operator = CloudRunExecuteJobOperator(
        task_id=f'deduplicate_table_job_{table_name}',
        project_id=project_id,
        region='us-central1',
        job_name='ccc-omop-file-processor-harmonize-vocab-job',
        overrides={
            'container_overrides': [{
                'env': [
                    {'name': 'FILE_PATH', 'value': json.dumps(table_config)},
                    {'name': 'VOCAB_VERSION', 'value': constants.OMOP_TARGET_VOCAB_VERSION},
                    {'name': 'OMOP_VERSION', 'value': constants.OMOP_TARGET_CDM_VERSION},
                    {'name': 'SITE', 'value': site},
                    {'name': 'PROJECT_ID', 'value': project_id},
                    {'name': 'DATASET_ID', 'value': table_config['dataset_id']},
                    {'name': 'STEP', 'value': constants.DEDUPLICATE_SINGLE_TABLE}
                ]
            }]
        },
        deferrable=False
    )

    operator.execute(context=context)


def run_generate_derived_table_job(
    site: str,
    gcs_bucket: str,
    delivery_date: str,
    table_name: str,
    vocab_version: str,
    project_id: str,
    context
) -> None:
    """
    Execute derived table generation as a Cloud Run Job.

    Generates derived tables (observation_period, condition_era, drug_era) from
    harmonized OMOP CDM data.

    Args:
        site: Site identifier
        gcs_bucket: GCS bucket path for the site
        delivery_date: Delivery date (YYYY-MM-DD format)
        table_name: Name of derived table to generate
        vocab_version: Vocabulary version
        project_id: GCP project ID
        context: Airflow task context
    """
    utils.logger.info(f"Executing generate_derived_table job: {table_name} for {site}")

    operator = CloudRunExecuteJobOperator(
        task_id=f'generate_derived_table_job_{table_name}',
        project_id=project_id,
        region='us-central1',
        job_name='ccc-omop-file-processor-generate-derived-tables-job',
        overrides={
            'container_overrides': [{
                'env': [
                    {'name': 'SITE', 'value': site},
                    {'name': 'GCS_BUCKET', 'value': gcs_bucket},
                    {'name': 'DELIVERY_DATE', 'value': delivery_date},
                    {'name': 'TABLE_NAME', 'value': table_name},
                    {'name': 'VOCAB_VERSION', 'value': vocab_version}
                ]
            }]
        },
        deferrable=False
    )

    operator.execute(context=context)
