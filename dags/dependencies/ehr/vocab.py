from dependencies.ehr import constants, utils


def load_vocabulary_table_gcs_to_bq(vocab_version: str, table_file_name: str, project_id: str, dataset_id: str) -> None:
    utils.logger.info(f"Loading {table_file_name} vocabulary table to {project_id}.{dataset_id}")
    utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="load_target_vocab",
        json_data={
            "vocab_version": vocab_version,
            "table_file_name": table_file_name,
            "project_id": project_id,
            "dataset_id": dataset_id,
        }
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
    step: str
) -> None:
    """
    Execute a single vocabulary harmonization step.
    
    Args:
        vocab_version: Target vocabulary version
        omop_version: Target OMOP CDM version
        file_path: Path to the file to harmonize
        site: Site identifier
        project_id: Google Cloud project ID
        dataset_id: BigQuery dataset ID
        step: The harmonization step to execute (source_target, target_remap, etc.)
    """
    utils.logger.info(f"Executing vocabulary harmonization step '{step}' for {file_path}")
    
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
        timeout=(constants.DEFAULT_CONNECTION_TIMEOUT_SEC, constants.VOCAB_TIMEOUT_SEC)
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
    dataset_id: str
) -> list[dict]:
    """
    Discover all tables that need primary key deduplication.

    Args:
        file_path: Dummy file path (for API compatibility)
        site: Site identifier
        project_id: Google Cloud project ID
        dataset_id: BigQuery dataset ID

    Returns:
        List of table configuration dictionaries for parallel processing
    """
    utils.logger.info(f"Discovering tables for deduplication for site {site}")

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
        timeout=(constants.DEFAULT_CONNECTION_TIMEOUT_SEC, constants.VOCAB_TIMEOUT_SEC)
    )

    # Extract table configs from the response (response is already parsed JSON)
    table_configs = response.get('table_configs', []) if response else []
    utils.logger.info(f"Discovered {len(table_configs)} table(s) for deduplication")

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
    """
    import json

    table_name = table_config.get('table_name', 'unknown')
    site = table_config.get('site', 'unknown')
    project_id = table_config['project_id']
    dataset_id = table_config['dataset_id']

    utils.logger.info(f"Deduplicating primary keys for table {table_name} from site {site}")

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
        timeout=(constants.DEFAULT_CONNECTION_TIMEOUT_SEC, constants.VOCAB_TIMEOUT_SEC)
    )
