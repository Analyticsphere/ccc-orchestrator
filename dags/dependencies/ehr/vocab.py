from dependencies.ehr import constants, utils


def load_vocabulary_table_gcs_to_bq(
    vocab_version: str,
    table_file_name: str,
    project_id: str,
    dataset_id: str,
    site: str = None,
    delivery_date: str = None
) -> None:
    """Load a vocabulary table from GCS to BigQuery.

    Args:
        vocab_version: Vocabulary version
        table_file_name: Vocabulary table file name
        project_id: BigQuery project ID
        dataset_id: BigQuery dataset ID
        site: Optional site identifier for logging context
        delivery_date: Optional delivery date for logging context
    """
    
    log_ctx = utils.format_log_context(site=site, delivery_date=delivery_date, file=table_file_name)
    utils.logger.info(f"{log_ctx}Loading vocabulary table to BigQuery: {project_id}.{dataset_id}")

    utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="load_target_vocab",
        json_data={
            "vocab_version": vocab_version,
            "table_file_name": table_file_name,
            "project_id": project_id,
            "dataset_id": dataset_id,
        },
        site=site,
        delivery_date=delivery_date,
        file=table_file_name
    )


def create_optimized_vocab() -> None:
    utils.logger.info(f"Creating optimized version of {constants.OMOP_TARGET_VOCAB_VERSION} if required")

    utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="create_optimized_vocab",
        json_data={
            "vocab_version": constants.OMOP_TARGET_VOCAB_VERSION
        }
    )


def harmonize_vocab_step(
    file_path: str,
    site: str,
    project_id: str,
    dataset_id: str,
    step: str,
    delivery_date: str = None
) -> None:
    """
    Execute a single vocabulary harmonization step.

    Args:
        file_path: Path to the file to harmonize
        site: Site identifier
        project_id: Google Cloud project ID
        dataset_id: BigQuery dataset ID
        step: The harmonization step to execute (source_target, target_remap, etc.)
        delivery_date: Optional delivery date for logging context
    """
    

    # Extract table name from file_path if available
    file_name = file_path.split('/')[-1] if file_path else None
    log_ctx = utils.format_log_context(site=site, delivery_date=delivery_date, file=file_name)
    utils.logger.info(f"{log_ctx}Executing vocabulary harmonization step: {step}")

    utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="harmonize_vocab",
        json_data={
            "file_path": file_path,
            "vocab_version": constants.OMOP_TARGET_VOCAB_VERSION,
            "vocab_gcs_path": constants.OMOP_VOCAB_GCS_PATH,
            "omop_version": constants.OMOP_TARGET_CDM_VERSION,
            "site": site,
            "project_id": project_id,
            "dataset_id": dataset_id,
            "step": step
        },
        timeout=(constants.DEFAULT_CONNECTION_TIMEOUT_SEC, constants.VOCAB_TIMEOUT_SEC),
        site=site,
        delivery_date=delivery_date,
        file=file_name
    )

def should_harmonize_table(table_name):
    """
    Determine if a table should be harmonized based on its name.

    Args:
        table_name: The name of the table to check
    Returns:
        bool: True if the table should be harmonized, False otherwise
    """
    return table_name in constants.VOCAB_HARMONIZED_TABLES


def discover_tables_for_dedup(
    file_path: str,
    site: str,
    project_id: str,
    dataset_id: str,
    delivery_date: str = None
) -> list[dict]:
    """
    Discover all tables that need primary key deduplication.

    Args:
        file_path: Dummy file path (for API compatibility)
        site: Site identifier
        project_id: Google Cloud project ID
        dataset_id: BigQuery dataset ID
        delivery_date: Optional delivery date for logging context

    Returns:
        List of table configuration dictionaries for parallel processing
    """
    
    log_ctx = utils.format_log_context(site=site, delivery_date=delivery_date)
    utils.logger.info(f"{log_ctx}Discovering tables requiring primary key deduplication")

    response = utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="harmonize_vocab",
        json_data={
            "file_path": file_path,
            "vocab_version": constants.OMOP_TARGET_VOCAB_VERSION,
            "vocab_gcs_path": constants.OMOP_VOCAB_GCS_PATH,
            "omop_version": constants.OMOP_TARGET_CDM_VERSION,
            "site": site,
            "project_id": project_id,
            "dataset_id": dataset_id,
            "step": constants.DISCOVER_TABLES_FOR_DEDUP
        },
        timeout=(constants.DEFAULT_CONNECTION_TIMEOUT_SEC, constants.VOCAB_TIMEOUT_SEC),
        site=site,
        delivery_date=delivery_date
    )

    # Extract table configs from the response (response is already parsed JSON)
    table_configs = response.get('table_configs', []) if response else []
    utils.logger.info(f"{log_ctx}Discovered {len(table_configs)} table(s) requiring deduplication")

    return table_configs


def deduplicate_single_table(table_config: dict) -> None:
    """
    Deduplicate primary keys for a single table.

    Args:
        table_config: Dictionary containing table configuration with keys:
            - table_name: Name of the table
            - file_path: Path to the consolidated parquet file
            - site: Site identifier
            - project_id: Google Cloud project ID
            - dataset_id: BigQuery dataset ID
            - cdm_version: CDM version
            - delivery_date: Optional delivery date
    """
    import json

    

    table_name = table_config.get('table_name', 'unknown')
    site = table_config.get('site', 'unknown')
    project_id = table_config['project_id']
    dataset_id = table_config['dataset_id']
    delivery_date = table_config.get('delivery_date')

    log_ctx = utils.format_log_context(site=site, delivery_date=delivery_date, file=table_name)
    utils.logger.info(f"{log_ctx}Deduplicating primary keys in table")

    # Pass the table config as JSON in the file_path parameter
    utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="harmonize_vocab",
        json_data={
            "file_path": json.dumps(table_config),
            "vocab_version": constants.OMOP_TARGET_VOCAB_VERSION,
            "vocab_gcs_path": constants.OMOP_VOCAB_GCS_PATH,
            "omop_version": table_config.get('cdm_version', constants.OMOP_TARGET_CDM_VERSION),
            "site": site,
            "project_id": project_id,
            "dataset_id": dataset_id,
            "step": constants.DEDUPLICATE_SINGLE_TABLE
        },
        timeout=(constants.DEFAULT_CONNECTION_TIMEOUT_SEC, constants.VOCAB_TIMEOUT_SEC),
        site=site,
        delivery_date=delivery_date,
        file=table_name
    )
