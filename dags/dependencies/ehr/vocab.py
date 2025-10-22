import time

from dependencies.ehr import utils, constants


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
        },
        timeout=(60, 3600)
    )


def create_optimized_vocab(vocab_version: str) -> None:
    utils.logger.info(f"Creating optimized version of {vocab_version} if required")

    utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="create_optimized_vocab",
        json_data={
            "vocab_version": vocab_version
        },
        timeout=(60, 3600)
    )

def harmonize(vocab_version: str, omop_version: str, file_path: str, site: str, project_id: str, dataset_id: str) -> None:
    """
    Simple harmonization function that makes a single API call.
    Kept for backward compatibility.
    """
    utils.logger.info(f"Standardizing {file_path} to vocabulary version {vocab_version}")

    utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="harmonize_vocab",
        json_data={
            "vocab_version": vocab_version,
            "omop_version": omop_version,
            "file_path": file_path,
            "site": site,
            "project_id": project_id,
            "dataset_id": dataset_id
        },
        timeout=(60, 3600)
    )

def harmonize_with_polling(vocab_version: str, omop_version: str, file_path: str, site: str, 
                          project_id: str, dataset_id: str, bucket_name: str, delivery_date: str, 
                          max_retries: int = 60) -> None:
    """
    Advanced harmonization function that submits a job and polls for completion.
    
    Args:
        vocab_version: Target vocabulary version
        omop_version: Target OMOP CDM version 
        file_path: Path to the file to harmonize
        site: Site identifier
        project_id: Google Cloud project ID
        dataset_id: BigQuery dataset ID
        bucket_name: Default GCS bucket if not provided in response
        delivery_date: Delivery date of the data
        max_retries: Maximum number of polling attempts
    
    Raises:
        Exception: If the job fails or times out
    """
    utils.logger.warning(f"Starting vocabulary harmonization job for {file_path}")
    
    # Start the harmonization job
    response = utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="harmonize_vocab",
        json_data={
            "vocab_version": vocab_version,
            "omop_version": omop_version,
            "file_path": file_path,
            "site": site,
            "project_id": project_id,
            "dataset_id": dataset_id
        },
        timeout=(60, 3600)
    )
    
    if not isinstance(response, dict) or 'job_id' not in response:
        raise Exception(f"Invalid response from harmonize_vocab API: {response}")
    
    job_id = response.get('job_id')
    bucket = response.get('bucket', bucket_name)
    
    utils.logger.info(f"Harmonization job {job_id} queued, processing steps")
    
    # Process each step of the job
    for attempt in range(max_retries):
        utils.logger.info(f"Processing step for job {job_id} - attempt {attempt+1}/{max_retries}")
        
        step_response = utils.make_api_call(
            url=constants.OMOP_PROCESSOR_ENDPOINT,
            endpoint="harmonize_vocab_process_step",
            json_data={
                "job_id": job_id,
                "bucket": bucket,
                "delivery_date": delivery_date
            },
            timeout=(60, 3600)
        )
        
        if step_response.get('status') == 'completed':
            utils.logger.info(f"Harmonization job {job_id} completed successfully")
            return
        elif step_response.get('status') == 'error':
            error_msg = step_response.get('error', 'Unknown error')
            raise Exception(f"Harmonization job {job_id} failed: {error_msg}")
            
        utils.logger.info(f"Completed step {step_response.get('current_step')}, continuing to next step")
        
        # Small delay between steps
        time.sleep(2)
    
    # If we get here, we've exceeded max attempts
    raise Exception(f"Harmonization job {job_id} timed out after {max_retries} steps")

def should_harmonize_table(table_name):
    """
    Determine if a table should be harmonized based on its name.
    
    Args:
        table_name: The name of the table to check        
    Returns:
        bool: True if the table should be harmonized, False otherwise
    """
    return table_name in constants.VOCAB_HARMONIZED_TABLES
