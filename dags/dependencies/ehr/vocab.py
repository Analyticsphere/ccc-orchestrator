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
