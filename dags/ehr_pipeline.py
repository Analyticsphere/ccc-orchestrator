from datetime import timedelta

import airflow  # type: ignore
import dependencies.bq as bq
import dependencies.constants as constants
import dependencies.file_config as file_config
import dependencies.omop as omop
import dependencies.processing as processing
import dependencies.utils as utils
import dependencies.validation as validation
from airflow import DAG  # type: ignore
from airflow.decorators import task  # type: ignore
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context  # type: ignore
from airflow.utils.dates import days_ago  # type: ignore
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(seconds=15),
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
    Task to verify EHR processing container is up.
    """
    utils.logger.info("Checking OMOP file processor API status")
    try:
        result = utils.check_service_health(constants.PROCESSOR_ENDPOINT)
        if result['status'] != 'healthy':
            error_msg = f"API health check failed. Status: {result['status']}"
            utils.logger.error(error_msg)
            raise Exception(error_msg)

        utils.logger.info(f"The API is healthy! Response: {result}")
    except Exception as e:
        utils.logger.error(f"API health check failed: {str(e)}")
        raise

@task()
def prepare_for_run() -> list[tuple[str, str]]:
    unprocessed_sites: list[tuple[str, str]] = []
    sites = utils.get_site_list()

    # Generate the optimized vocabulary files
    omop.create_optimized_vocab(constants.TARGET_VOCAB_VERSION, constants.VOCAB_REF_GCS_BUCKET)

    for site in sites:
        delivery_date_to_check = utils.get_most_recent_folder(site)
        site_log_entries = bq.get_bq_log_row(site, delivery_date_to_check)

        # If no log entry exists or the status indicates an error, mark as unprocessed.
        if not site_log_entries or site_log_entries[0]['status'] == constants.PIPELINE_ERROR_STRING:
            unprocessed_sites.append((site, delivery_date_to_check))

    return unprocessed_sites

@task.short_circuit
def check_for_unprocessed(unprocessed_sites: list[tuple[str, str]]) -> bool:
    """
    Returns False (which skips downstream tasks) if there are no unprocessed sites.
    """
    if not unprocessed_sites:
        utils.logger.info("No unprocessed sites found. Skipping processing tasks.")
        return False
    return True

@task()
def get_files(sites_to_process: list[tuple[str, str]]) -> list[dict]:
    """
    Obtains list of EHR data files that need to be processed.
    """
    utils.logger.info("Getting list of files to process")
    
    file_configs: list[dict] = []
    
    for site_to_process in sites_to_process:
        site, delivery_date = site_to_process

        site_config = utils.get_site_config_file()[constants.FileConfig.SITE.value][site]
        run_id = utils.get_run_id(get_current_context())

        bq.bq_log_start(
            site,
            delivery_date,
            site_config[constants.FileConfig.FILE_DELIVERY_FORMAT.value],
            site_config[constants.FileConfig.OMOP_VERSION.value],
            run_id
        )

        try:
            # get_file_list() may also create artifact buckets for the pipeline for a given site delivery.
            files = processing.get_file_list(site, delivery_date, site_config[constants.FileConfig.FILE_DELIVERY_FORMAT.value])

            for file in files:
                file_config_obj = file_config.FileConfig(site, file)
                file_configs.append(file_config_obj.to_dict())
        except Exception as e:
            error_msg = f"Unable to get file list: {str(e)}"
            utils.logger.error(error_msg)
            bq.bq_log_error(run_id, str(e))
            raise Exception(error_msg) from e

    return file_configs

@task(max_active_tis_per_dag=10, execution_timeout=timedelta(minutes=60))
def process_file(file_config: dict) -> None:
    """
    Create optimized version of incoming EHR data file.
    """
    site = file_config[constants.FileConfig.SITE.value]
    delivery_date = file_config[constants.FileConfig.DELIVERY_DATE.value]
    
    try:
        bq.bq_log_running(site, delivery_date)
        file_type = f"{file_config[constants.FileConfig.FILE_DELIVERY_FORMAT.value]}"
        gcs_file_path = utils.get_file_path(file_config)

        processing.process_file(file_type, gcs_file_path)
    except Exception as e:
        error_msg = f"Unable to process incoming file: {e}"
        utils.logger.error(error_msg)
        bq.bq_log_error(utils.get_run_id(get_current_context()), str(e))
        raise Exception(error_msg) from e

@task(max_active_tis_per_dag=10, execution_timeout=timedelta(minutes=60))
def validate_file(file_config: dict) -> None:
    """
    Validate file against OMOP CDM specifications.
    """
    site = file_config[constants.FileConfig.SITE.value]
    delivery_date = file_config[constants.FileConfig.DELIVERY_DATE.value]

    try:
        bq.bq_log_running(site, delivery_date)
        validation.validate_file(
            file_path=utils.get_file_path(file_config),
            omop_version=file_config[constants.FileConfig.OMOP_VERSION.value],
            gcs_path=file_config[constants.FileConfig.GCS_PATH.value],
            delivery_date=file_config[constants.FileConfig.DELIVERY_DATE.value]
        )
    except Exception as e:
        error_msg = f"Unable to validate file: {e}"
        utils.logger.error(error_msg)
        bq.bq_log_error(utils.get_run_id(get_current_context()), str(e))
        raise Exception(error_msg) from e

@task(max_active_tis_per_dag=10, execution_timeout=timedelta(minutes=60))
def normalize_file(file_config: dict) -> None:
    """
    Standardize OMOP data file structure.
    """
    site = file_config[constants.FileConfig.SITE.value]
    delivery_date = file_config[constants.FileConfig.DELIVERY_DATE.value]

    try:
        bq.bq_log_running(site, delivery_date)

        file_path = utils.get_file_path(file_config)
        omop_version = file_config[constants.FileConfig.OMOP_VERSION.value]

        processing.normalize_parquet_file(file_path, omop_version)
    except Exception as e:
        error_msg = f"Unable to fix file: {e}"
        utils.logger.error(error_msg)
        bq.bq_log_error(utils.get_run_id(get_current_context()), str(e))
        raise Exception(error_msg) from e

@task(max_active_tis_per_dag=10, execution_timeout=timedelta(minutes=60))
def cdm_upgrade(file_config: dict) -> None:
    """
    Upgrade CDM version (currently supports 5.3 -> 5.4)
    """
    site = file_config[constants.FileConfig.SITE.value]
    delivery_date = file_config[constants.FileConfig.DELIVERY_DATE.value]

    try:
        bq.bq_log_running(site, delivery_date)

        cdm_version = file_config[constants.FileConfig.OMOP_VERSION.value]
        file_path = utils.get_file_path(file_config)

        if cdm_version == constants.TARGET_CDM_VERSION:
            utils.logger.info(f"CDM version of {file_path} ({cdm_version}) matches upgrade target {constants.TARGET_CDM_VERSION}; upgrade not needed")
            pass
        elif cdm_version == "5.3":
            processing.upgrade_cdm(file_path, cdm_version, constants.TARGET_CDM_VERSION)
        else:
            utils.logger.error(f"OMOP CDM version {cdm_version} not supported")
            raise Exception(f"OMOP CDM version {cdm_version} not supported")
    except Exception as e:
        error_msg = f"Unable to upgrade file: {e}"
        utils.logger.error(error_msg)
        bq.bq_log_error(utils.get_run_id(get_current_context()), str(e))
        raise Exception(error_msg) from e
    
@task
def prepare_bq(sites_to_process: list[tuple[str, str]]) -> None:
    """
    Deletes files and tables from previous pipeline runs.
    """    
    for site_to_process in sites_to_process:
        try:
            site, delivery_date = site_to_process
            bq.bq_log_running(site, delivery_date)

            site_config = utils.get_site_config_file()
            project_id = site_config['site'][site][constants.FileConfig.PROJECT_ID.value]
            dataset_id = site_config['site'][site][constants.FileConfig.BQ_DATASET.value]

            # Delete all tables within the BigQuery dataset.
            bq.prep_dataset(project_id, dataset_id)
        except Exception as e:
            error_msg = f"Unable to prepare BigQuery: {e}"
            utils.logger.error(error_msg)
            bq.bq_log_error(utils.get_run_id(get_current_context()), str(e))
            raise Exception(error_msg) from e

@task(max_active_tis_per_dag=10, execution_timeout=timedelta(minutes=60))
def load_to_bq(file_config: dict) -> None:
    """
    Load OMOP data file to BigQuery table.
    """
    site = file_config[constants.FileConfig.SITE.value]
    delivery_date = file_config[constants.FileConfig.DELIVERY_DATE.value]

    try:
        bq.bq_log_running(site, delivery_date)

        gcs_file_path = utils.get_file_path(file_config)
        project_id = f"{file_config[constants.FileConfig.PROJECT_ID.value]}"
        dataset_id = f"{file_config[constants.FileConfig.BQ_DATASET.value]}"

        bq.load_parquet_to_bq(gcs_file_path, project_id, dataset_id)
    except Exception as e:
        error_msg = f"Unable to write to BigQuery: {e}"
        utils.logger.error(error_msg)
        bq.bq_log_error(utils.get_run_id(get_current_context()), str(e))
        raise Exception(error_msg) from e

@task
def final_cleanup(sites_to_process: list[tuple[str, str]]) -> None:
    """
    Perform final data cleanup here.
    """
    # Implement any cleanup logic if needed.
    utils.logger.info("Executing final cleanup tasks")

    for unprocessed_site in sites_to_process:
        site, delivery_date = unprocessed_site

        try:
            bq.bq_log_running(site, delivery_date)
            
            # Generate final data delivery report
            report_data = omop.generate_report_json(site, delivery_date)
            validation.generate_delivery_report(report_data)

            # Create empty tables for OMOP files not provided in delivery
            project_id = utils.get_site_config_file()[constants.FileConfig.SITE.value][site][constants.FileConfig.PROJECT_ID.value]
            dataset_id = utils.get_site_config_file()[constants.FileConfig.SITE.value][site][constants.FileConfig.BQ_DATASET.value]
            omop.create_missing_omop_tables(project_id, dataset_id, constants.TARGET_CDM_VERSION)
            
            # Add record to cdm_source table in BigQuery, if not provided by site
            cdm_source_data = omop.generate_cdm_source_json(site, delivery_date)
            omop.populate_cdm_source(cdm_source_data)

            # Add completed log entry to BigQuery tracking table
            bq.bq_log_complete(site, delivery_date)
        except Exception as e:
            error_msg = f"Unable to perform final cleanup: {e}"
            utils.logger.error(error_msg)
            bq.bq_log_error(utils.get_run_id(get_current_context()), str(e))
            raise Exception(error_msg) from e

@task(trigger_rule=TriggerRule.ALL_DONE, retries=0)
def log_done() -> None:
    # This final task will run regardless of previous task states.
    context = get_current_context()
    dag_run = context.get('dag_run')
    run_id = dag_run.run_id
    task_instances = dag_run.get_task_instances()
    
    failures_detected = False
    
    # Check if any tasks failed
    for ti in task_instances:
        if "fail" in ti.state.lower():
            bq.bq_log_error(run_id, constants.PIPELINE_DAG_FAIL_MESSAGE)
            failures_detected = True

    # Check if the dag_run itself is in a failed state
    if dag_run and "fail" in dag_run.state.lower():
        bq.bq_log_error(run_id, constants.PIPELINE_DAG_FAIL_MESSAGE)
        failures_detected = True
    
    # Raise an exception to fail this task (and thus the entire DAG)
    if failures_detected:
        utils.logger.error("Failures detected in DAG execution. Marking the DAG as failed.")
        raise AirflowException("DAG execution failed due to one or more task failures.")

# Define the DAG structure.
with dag:
    api_health_check = check_api_health()
    unprocessed_sites = prepare_for_run()
    sites_exist = check_for_unprocessed(unprocessed_sites)
    file_list = get_files(sites_to_process=unprocessed_sites)
    
    # Expand the processing tasks across the list of file configurations.
    process_files = process_file.expand(file_config=file_list)
    validate_files = validate_file.expand(file_config=file_list)
    fix_data_file = normalize_file.expand(file_config=file_list)
    upgrade_file = cdm_upgrade.expand(file_config=file_list)
    
    clean_bq = prepare_bq(sites_to_process=unprocessed_sites)
    load_file = load_to_bq.expand(file_config=file_list)
    cleanup = final_cleanup(sites_to_process=unprocessed_sites)

    # Final log_done task runs regardless of task outcomes.
    all_done = log_done()
    
    # Set task dependencies.
    api_health_check >> unprocessed_sites >> sites_exist >> file_list
    file_list >> process_files >> validate_files >> fix_data_file >> upgrade_file >> clean_bq 
    clean_bq >> load_file >> cleanup >> all_done

