import airflow
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task, task_group
from airflow.operators.python import get_current_context

from datetime import datetime, timedelta
import dependencies.utils as utils
import dependencies.constants as constants
import dependencies.processing as processing
import dependencies.validation as validation
import dependencies.bq as bq
import dependencies.file_config as file_config
import sys
from typing import List, Tuple


default_args = {
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'owner': 'airflow'
}

dag = DAG(
    'ehr-pipeline',
    default_args=default_args,
    description='Pipeline for processing EHR OMOP data files',
    start_date=days_ago(1)
)

@task()
def check_api_health() -> None:
    """
    Task to verify EHR processing container is up
    """
    utils.logger.info("Checking OMOP file processor API status")
    try:
        result = utils.check_service_health(constants.PROCESSOR_ENDPOINT)
        if result['status'] != 'healthy':
            utils.logger.error(f"API health check failed. Status: {result['status']}")
            sys.exit(1)

        utils.logger.info(f"The API is healthy! Response: {result}")
    except Exception as e:
        utils.logger.error(f"API health check failed: {str(e)}")
        sys.exit(1)

@task()
def get_unprocessed_sites() -> List[Tuple[str, str]]:
    unprocessed_sites: List[Tuple[str, str]] = []
    sites = utils.get_site_list()

    for site in sites:
        date_to_check = utils.get_most_recent_folder(site)
        site_log_entries = bq.get_bq_log_row(site, date_to_check)

        # If no log entry exists or the status is not error, mark as unprocessed.
        # If status is completed or running, we should not mark as unprocessed
        if not site_log_entries or site_log_entries[0]['status'] == constants.PIPELINE_ERROR_STRING:
            unprocessed_sites.append((site, date_to_check))

    return unprocessed_sites

@task.short_circuit
def check_for_unprocessed(unprocessed_sites: List[Tuple[str, str]]) -> bool:
    """
    Returns False (which skips downstream tasks) if there are no unprocessed sites.
    """
    if not unprocessed_sites:
        utils.logger.info("No unprocessed sites found. Skipping processing tasks.")
        return False
    return True

@task()
def get_files(sites_to_process: List[Tuple[str, str]]) -> list[dict]:
    """
    Obtains list of EHR data files that need to be processed
    """
    utils.logger.info("Getting list of files to process")
    
    # Store list of files from unprocessed deliveries
    file_configs: list[dict] = []

    # Retrieve the current Airflow context and extract the run_id
    context = get_current_context()
    run_id = context['dag_run'].run_id if context.get('dag_run') else "unknown"
    
    for site_to_process in sites_to_process:
        site, delivery_date = site_to_process

        site_config = utils.get_site_config_file()[constants.FileConfig.SITE.value][site]

        bq.bq_log_start(
            site,
            delivery_date,
            site_config[constants.FileConfig.FILE_DELIVERY_FORMAT.value],
            site_config[constants.FileConfig.OMOP_VERSION.value],
            run_id
        )

        try:
            # get_file_list() also creates artifact buckets for the pipeline for a given site delivery
            files = processing.get_file_list(site, delivery_date)

            for file in files:
                # Create file configuration dictionaries for each file from a site
                file_config_obj = file_config.FileConfig(site, file)
                file_configs.append(file_config_obj.to_dict())
        except Exception as e:
            utils.logger.error(f"Unable to get file list: {str(e)}")
            bq.bq_log_error(site, delivery_date, e)
            sys.exit(1)

    return file_configs


@task(max_active_tis_per_dag=10, execution_timeout=timedelta(minutes=60))
def process_incoming_file(file_config: dict) -> None:
    """
    Create optimized version of incoming EHR data file
    """
    site = file_config[constants.FileConfig.SITE.value]
    delivery_date = file_config[constants.FileConfig.DELIVERY_DATE.value]
    
    try:
        bq.bq_log_running(site, delivery_date)
        file_type = f"{file_config[constants.FileConfig.FILE_DELIVERY_FORMAT.value]}"
        gcs_file_path = f"{file_config[constants.FileConfig.GCS_PATH.value]}/{file_config[constants.FileConfig.DELIVERY_DATE.value]}/{file_config[constants.FileConfig.FILE_NAME.value]}"

        processing.process_file(file_type, gcs_file_path)
    except Exception as e:
        utils.logger.error(f"Unable to processing incoming file: {e}")
        bq.bq_log_error(site, delivery_date, e)
        sys.exit(1)

@task(max_active_tis_per_dag=10, execution_timeout=timedelta(minutes=60))
def validate_file(file_config: dict) -> None:
    """
    Validate file against OMOP CDM specificiations
    """
    site = file_config[constants.FileConfig.SITE.value]
    delivery_date = file_config[constants.FileConfig.DELIVERY_DATE.value]

    try:
        bq.bq_log_running(site, delivery_date)
        validation.validate_file(
            file_path=f"gs://{file_config[constants.FileConfig.GCS_PATH.value]}/{file_config[constants.FileConfig.DELIVERY_DATE.value]}/{file_config[constants.FileConfig.FILE_NAME.value]}", 
            omop_version=file_config[constants.FileConfig.OMOP_VERSION.value],
            gcs_path=file_config[constants.FileConfig.GCS_PATH.value],
            delivery_date=file_config[constants.FileConfig.DELIVERY_DATE.value]
            )
    except Exception as e:
        utils.logger.error(f"Unable to validate file: {e}")
        bq.bq_log_error(site, delivery_date, e)
        sys.exit(1)

@task(max_active_tis_per_dag=10, execution_timeout=timedelta(minutes=60))
def fix_file(file_config: dict) -> None:
    """
    Standardize OMOP data file structure
    """
    site = file_config[constants.FileConfig.SITE.value]
    delivery_date = file_config[constants.FileConfig.DELIVERY_DATE.value]

    try:
        bq.bq_log_running(site, delivery_date)
        file_name = file_config[constants.FileConfig.FILE_NAME.value].replace(file_config[constants.FileConfig.FILE_DELIVERY_FORMAT.value], '')
        file_path = f"{file_config[constants.FileConfig.GCS_PATH.value]}/{file_config[constants.FileConfig.DELIVERY_DATE.value]}/{constants.ArtifactPaths.CONVERTED_FILES.value}{file_name}{constants.PARQUET}"

        omop_version = file_config[constants.FileConfig.OMOP_VERSION.value]

        utils.logger.info(f"Fixing Parquet file gs://{file_path}")
        processing.fix_parquet_file(file_path, omop_version)
    except Exception as e:
        utils.logger.error(f"Unable to fix file: {e}")
        bq.bq_log_error(site, delivery_date, e)
        sys.exit(1)

@task
def prepare_bq(sites_to_process: List[Tuple[str, str]]) -> None:
    """
    Deletes files and tables from previous pipeline runs
    """    
    for site_to_process in sites_to_process:
        try:
            site, delivery_date = site_to_process
            bq.bq_log_running(site, delivery_date)

            site_config = utils.get_site_config_file()

            project_id = site_config['site'][site][constants.FileConfig.PROJECT_ID.value]
            dataset_id = site_config['site'][site][constants.FileConfig.BQ_DATASET.value]

            # Delete all tables within BigQuery dataset
            bq.prep_dataset(project_id, dataset_id)
        except Exception as e:
            utils.logger.error(f"Unable to prepare BigQuery: {e}")
            bq.bq_log_error(site, delivery_date, e)
            sys.exit(1)

@task(max_active_tis_per_dag=10, execution_timeout=timedelta(minutes=60))
def load_to_bq(file_config: dict) -> None:
    """
    Load OMOP data file to BigQuery table
    """
    site = file_config[constants.FileConfig.SITE.value]
    delivery_date = file_config[constants.FileConfig.DELIVERY_DATE.value]
    try:
        gcs_file_path = f"{file_config[constants.FileConfig.GCS_PATH.value]}/{file_config[constants.FileConfig.DELIVERY_DATE.value]}/{file_config[constants.FileConfig.FILE_NAME.value]}"
        project_id = f"{file_config[constants.FileConfig.PROJECT_ID.value]}"
        dataset_id = f"{file_config[constants.FileConfig.BQ_DATASET.value]}"

        bq.load_parquet_to_bq(gcs_file_path, project_id, dataset_id)
    except Exception as e:
        utils.logger.error(f"Unable to write to BigQuery: {e}")
        bq.bq_log_error(site, delivery_date, e)
        sys.exit(1)

@task
def final_cleanup(sites_to_process: List[Tuple[str, str]]) -> None:
    
    for unprocessed_site in sites_to_process:
        site, delivery_date = unprocessed_site
        bq.bq_log_complete(site, delivery_date)

# @task(max_active_tis_per_dag=10)
# def dummy_testing_task(file_config: dict) -> None:
#     utils.logger.info(f"Going to validate schema of gs://{file_config[constants.FileConfig.GCS_PATH.value]}/{file_config[constants.FileConfig.DELIVERY_DATE.value]}/{file_config[constants.FileConfig.FILE_NAME.value]} against OMOP v{file_config[constants.FileConfig.OMOP_VERSION.value]}")
#     utils.logger.info(f"Will write to BQ dataset {file_config[constants.FileConfig.PROJECT_ID.value]}.{file_config[constants.FileConfig.BQ_DATASET.value]}")

with dag:
    api_health_check = check_api_health()
    unprocessed_sites = get_unprocessed_sites()
    # This task will short-circuit (skip downstream tasks) if no sites are returned.
    sites_exist = check_for_unprocessed(unprocessed_sites)
    
    file_list = get_files(sites_to_process=unprocessed_sites)
    process_files = process_incoming_file.expand(file_config=file_list)
    validate_files = validate_file.expand(file_config=file_list)
    fix_data_file = fix_file.expand(file_config=file_list)
    clean_bq = prepare_bq(sites_to_process=unprocessed_sites)
    load_file = load_to_bq.expand(file_config=file_list)
    cleanup = final_cleanup(sites_to_process=unprocessed_sites)
    
    # Define dependencies
    api_health_check >> unprocessed_sites >> sites_exist >> file_list
    file_list >> process_files >> validate_files >> fix_data_file >> clean_bq >> load_file >> cleanup