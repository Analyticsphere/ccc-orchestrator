import os
from datetime import timedelta

import airflow  # type: ignore
import dependencies.ehr.analysis as analysis
import dependencies.ehr.bq as bq
import dependencies.ehr.constants as constants
import dependencies.ehr.file_config as file_config
import dependencies.ehr.omop as omop
import dependencies.ehr.processing as processing
import dependencies.ehr.utils as utils
import dependencies.ehr.validation as validation
import dependencies.ehr.vocab as vocab
from airflow import DAG  # type: ignore
from airflow.decorators import task  # type: ignore
from airflow.exceptions import AirflowSkipException  # type: ignore
from airflow.exceptions import AirflowException  # type: ignore
from airflow.utils.dates import days_ago  # type: ignore
from airflow.utils.task_group import TaskGroup  # type: ignore
from airflow.utils.trigger_rule import TriggerRule  # type: ignore
from dependencies.ehr.dag_helpers import (SiteConfig, TaskContext,
                                          log_task_execution)

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
        result = utils.check_service_health(base_url=constants.OMOP_PROCESSOR_ENDPOINT)
        if result['status'] != 'healthy':
            raise Exception(f"API health check failed. Status: {result['status']}")

        utils.logger.info(f"The API is healthy! Response: {result}")
    except Exception as e:
        utils.logger.error(f"API health check failed: {str(e)}")
        raise


@task()
def id_sites_to_process() -> list[tuple[str, str]]:
    """
    Identify sites with unprocessed or errored deliveries.
    Also generates optimized vocabulary files needed for processing.
    """
    unprocessed_sites: list[tuple[str, str]] = []
    sites = utils.get_site_list()

    # Generate the optimized vocabulary files
    vocab.create_optimized_vocab()

    for site in sites:
        delivery_date_to_check = utils.get_most_recent_folder(site=site)
        site_log_entries = bq.get_bq_log_row(site=site, delivery_date=delivery_date_to_check)

        # If no log entry exists or the status indicates an error, mark as unprocessed.
        if not site_log_entries or site_log_entries[0]['status'] == constants.PIPELINE_ERROR_STRING:
            unprocessed_sites.append((site, delivery_date_to_check))

    return unprocessed_sites


@task.short_circuit
def end_if_all_processed(unprocessed_sites: list[tuple[str, str]]) -> bool:
    """
    Returns False (which skips downstream tasks) if there are no sites to process.
    """
    if not unprocessed_sites:
        utils.logger.info("No unprocessed sites found. Skipping processing tasks.")
        return False
    return True


@task()
def get_unprocessed_files(sites_to_process: list[tuple[str, str]]) -> list[dict]:
    """
    Obtains list of EHR data files that need to be processed.
    """
    utils.logger.info("Getting list of files to process")

    file_configs: list[dict] = []

    for site, delivery_date in sites_to_process:
        config = SiteConfig(site=site)
        run_id = TaskContext.get_run_id()

        bq.bq_log_start(
            site=site,
            delivery_date=delivery_date,
            file_type=config.file_delivery_format,
            omop_version=config.omop_version,
            run_id=run_id
        )

        try:
            # get_file_list() may also create artifact buckets for the pipeline for a given site delivery.
            files = processing.get_file_list(
                site=site,
                delivery_date=delivery_date,
                file_format=config.file_delivery_format
            )

            for file in files:
                file_config_obj = file_config.FileConfig(
                    site=site,
                    delivery_date=delivery_date,
                    source_file=file
                )
                file_configs.append(file_config_obj.to_dict())
        except Exception as e:
            error_msg = f"Unable to get file list: {str(e)}"
            bq.bq_log_error(
                site=site,
                delivery_date=delivery_date,
                run_id=run_id,
                message=str(e)
            )
            raise Exception(error_msg) from e

    return file_configs


@task(max_active_tis_per_dag=24, execution_timeout=timedelta(minutes=30))
@log_task_execution()
def convert_file(file_config_dict: dict) -> None:
    """
    Create optimized version of incoming EHR data file.
    """
    fc = file_config.FileConfig.from_dict(config_dict=file_config_dict)
    processing.process_file(file_type=fc.file_delivery_format, gcs_file_path=fc.file_path)


@task(max_active_tis_per_dag=24)
@log_task_execution()
def validate_file(file_config_dict: dict) -> None:
    """
    Validate file against OMOP CDM specifications.
    """
    fc = file_config.FileConfig.from_dict(config_dict=file_config_dict)
    validation.validate_file(
        file_path=fc.file_path,
        omop_version=fc.omop_version,
        gcs_path=fc.gcs_bucket,
        delivery_date=fc.delivery_date
    )


@task(max_active_tis_per_dag=24, execution_timeout=timedelta(minutes=30))
@log_task_execution()
def normalize_file(file_config_dict: dict) -> None:
    """
    Standardize OMOP data file structure.
    """
    fc = file_config.FileConfig.from_dict(config_dict=file_config_dict)
    processing.normalize_parquet_file(
        file_path=fc.file_path,
        cdm_version=fc.omop_version,
        date_format=fc.date_format,
        datetime_format=fc.datetime_format
    )


@task(max_active_tis_per_dag=24)
@log_task_execution()
def cdm_upgrade(file_config_dict: dict) -> None:
    """
    Upgrade CDM version (currently supports 5.3 -> 5.4)
    """
    fc = file_config.FileConfig.from_dict(config_dict=file_config_dict)

    if fc.omop_version == constants.OMOP_TARGET_CDM_VERSION:
        utils.logger.info(
            f"CDM version of {fc.file_path} ({fc.omop_version}) matches "
            f"upgrade target {constants.OMOP_TARGET_CDM_VERSION}; upgrade not needed"
        )
    else:
        omop.upgrade_cdm(
            file_path=fc.file_path,
            cdm_version=fc.omop_version,
            target_cdm_version=constants.OMOP_TARGET_CDM_VERSION
        )


@task(max_active_tis_per_dag=24, trigger_rule="none_failed", execution_timeout=timedelta(minutes=constants.VOCAB_TIME_MIN))
@log_task_execution()
def harmonize_vocab_source_target(file_config_dict: dict) -> None:
    """
    Step 1: Execute source_target vocabulary harmonization.
    """
    fc = file_config.FileConfig.from_dict(config_dict=file_config_dict)

    # Check if this table should be harmonized
    if not vocab.should_harmonize_table(table_name=fc.table_name):
        utils.logger.info(f"File {fc.table_name} is not a clinical data table and does not need vocabulary harmonization")
        raise AirflowSkipException

    # Get configuration parameters
    config = SiteConfig(site=fc.site)

    # Execute source_target step
    vocab.harmonize_vocab_step(
        file_path=fc.file_path,
        site=fc.site,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        step=constants.SOURCE_TARGET
    )


@task(max_active_tis_per_dag=24, trigger_rule="none_failed", execution_timeout=timedelta(minutes=constants.VOCAB_TIME_MIN))
@log_task_execution()
def harmonize_vocab_target_remap(file_config_dict: dict) -> None:
    """
    Step 2: Execute target_remap vocabulary harmonization.
    """
    fc = file_config.FileConfig.from_dict(config_dict=file_config_dict)

    # Check if this table should be harmonized
    if not vocab.should_harmonize_table(table_name=fc.table_name):
        raise AirflowSkipException

    # Get configuration parameters
    config = SiteConfig(site=fc.site)

    # Execute target_remap step
    vocab.harmonize_vocab_step(
        file_path=fc.file_path,
        site=fc.site,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        step=constants.TARGET_REMAP
    )


@task(max_active_tis_per_dag=24, trigger_rule="none_failed", execution_timeout=timedelta(minutes=constants.VOCAB_TIME_MIN))
@log_task_execution()
def harmonize_vocab_target_replacement(file_config_dict: dict) -> None:
    """
    Step 3: Execute target_replacement vocabulary harmonization.
    """
    fc = file_config.FileConfig.from_dict(config_dict=file_config_dict)

    # Check if this table should be harmonized
    if not vocab.should_harmonize_table(table_name=fc.table_name):
        raise AirflowSkipException

    # Get configuration parameters
    config = SiteConfig(site=fc.site)

    # Execute target_replacement step
    vocab.harmonize_vocab_step(
        file_path=fc.file_path,
        site=fc.site,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        step=constants.TARGET_REPLACEMENT
    )


@task(max_active_tis_per_dag=24, trigger_rule="none_failed", execution_timeout=timedelta(minutes=constants.VOCAB_TIME_MIN))
@log_task_execution()
def harmonize_vocab_domain_check(file_config_dict: dict) -> None:
    """
    Step 4: Execute domain_check vocabulary harmonization.
    """
    fc = file_config.FileConfig.from_dict(config_dict=file_config_dict)

    # Check if this table should be harmonized
    if not vocab.should_harmonize_table(table_name=fc.table_name):
        raise AirflowSkipException

    # Get configuration parameters
    config = SiteConfig(site=fc.site)

    # Execute domain_check step
    vocab.harmonize_vocab_step(
        file_path=fc.file_path,
        site=fc.site,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        step=constants.DOMAIN_CHECK
    )


@task(max_active_tis_per_dag=24, trigger_rule="none_failed", execution_timeout=timedelta(minutes=constants.VOCAB_TIME_MIN))
@log_task_execution()
def harmonize_vocab_omop_etl(file_config_dict: dict) -> None:
    """
    Step 5: Execute omop_etl vocabulary harmonization and load to BigQuery.
    """
    fc = file_config.FileConfig.from_dict(config_dict=file_config_dict)

    # Check if this table should be harmonized
    if not vocab.should_harmonize_table(table_name=fc.table_name):
        raise AirflowSkipException

    # Get configuration parameters
    config = SiteConfig(site=fc.site)

    # Execute omop_etl step
    vocab.harmonize_vocab_step(
        file_path=fc.file_path,
        site=fc.site,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        step=constants.OMOP_ETL
    )


@task(max_active_tis_per_dag=24, trigger_rule="none_failed", execution_timeout=timedelta(minutes=constants.VOCAB_TIME_MIN))
@log_task_execution()
def harmonize_vocab_consolidate(site_to_process: tuple[str, str]) -> None:
    """
    Step 6: Combine and deduplicate ETL files after vocabulary harmonization.
    This occurs once per site; not per file
    """
    site, delivery_date = site_to_process
    config = SiteConfig(site=site)

    # Execute consolidation step on the site's OMOP-to-OMOP ETL files
    vocab.harmonize_vocab_step(
        file_path=f"gs://{config.gcs_bucket}/{delivery_date}/dummy_value_for_consolidation",
        site=site,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        step=constants.CONSOLIDATE_ETL
    )


@task(max_active_tis_per_dag=24, trigger_rule="none_failed", execution_timeout=timedelta(minutes=constants.VOCAB_TIME_MIN))
@log_task_execution()
def harmonize_vocab_primary_keys_dedup(site_to_process: tuple[str, str]) -> None:
    """
    Step 7: Identify and correct duplicate primary keys in ETL files after vocabulary harmonization.
    This occurs once per site; not per file
    """
    site, delivery_date = site_to_process
    config = SiteConfig(site=site)

    # Execute primary key deduplication step on the site's OMOP-to-OMOP ETL files
    vocab.harmonize_vocab_step(
        file_path=f"gs://{config.gcs_bucket}/{delivery_date}/dummy_value_for_consolidation",
        site=site,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        step=constants.DEDUPLICATE_PRIMARY_KEYS
    )


@task(max_active_tis_per_dag=10, trigger_rule="none_failed")
@log_task_execution()
def prepare_bq(site_to_process: tuple[str, str]) -> None:
    """
    Deletes files and tables from previous pipeline runs.
    """
    site, delivery_date = site_to_process
    config = SiteConfig(site=site)

    # Delete all tables within the BigQuery dataset.
    bq.prep_dataset(project_id=config.project_id, dataset_id=config.dataset_id)


@task(max_active_tis_per_dag=10, trigger_rule="none_failed")
@log_task_execution()
def load_harmonized_tables(site_to_process: tuple[str, str]) -> None:
    """
    Load all harmonized vocabulary tables to BigQuery
    """
    site, delivery_date = site_to_process
    config = SiteConfig(site=site)

    bq.load_harmonized_tables_to_bq(
        gcs_bucket=config.gcs_bucket,
        delivery_date=delivery_date,
        project_id=config.project_id,
        dataset_id=config.dataset_id
    )
    

@task(max_active_tis_per_dag=10, trigger_rule="none_failed")
@log_task_execution(skip_running_log=True)
def load_target_vocab(site_to_process: tuple[str, str]) -> None:
    """
    Load all target vocabulary tables to BigQuery
    If the site's overwrite_site_vocab_with_standard is False, the site's vocab tables will overwrite default tables loaded by this task
    """
    site, _ = site_to_process
    config = SiteConfig(site=site)

    if config.overwrite_site_vocab_with_standard:
        for vocab_table in constants.VOCABULARY_TABLES:
            vocab.load_vocabulary_table_gcs_to_bq(
                vocab_version=constants.OMOP_TARGET_VOCAB_VERSION,
                table_file_name=vocab_table,
                project_id=config.project_id,
                dataset_id=config.dataset_id
            )


@task(max_active_tis_per_dag=24, trigger_rule="none_failed")
@log_task_execution()
def load_remaining(file_config_dict: dict) -> None:
    """
    Load OMOP data file to BigQuery table.
    """
    fc = file_config.FileConfig.from_dict(config_dict=file_config_dict)
    config = SiteConfig(site=fc.site)

    # Don't load standard vocabulary files if using site-specific vocabulary
    if config.overwrite_site_vocab_with_standard and fc.table_name in constants.VOCABULARY_TABLES:
        utils.logger.info(f"Skip loading {fc.table_name} to BigQuery")
        raise AirflowSkipException

    # Also skip vocab harmonized tables since they were already loaded in harmonize_vocab task
    elif fc.table_name in constants.VOCAB_HARMONIZED_TABLES:
        utils.logger.info(f"Skip loading {fc.table_name} to BigQuery")
        raise AirflowSkipException
    else:
        bq.load_individual_parquet_to_bq(
            file_path=fc.file_path,
            project_id=config.project_id,
            dataset_id=config.dataset_id,
            table_name=fc.table_name,
            write_type=constants.BQWriteTypes.PROCESSED_FILE
        )


@task(max_active_tis_per_dag=10, trigger_rule="none_failed")
@log_task_execution()
def derived_data_tables(site_to_process: tuple[str, str]) -> None:
    site, delivery_date = site_to_process
    config = SiteConfig(site=site)

    for dervied_table in constants.DERIVED_DATA_TABLES:
        omop.create_derived_data_table(
            site=site,
            gcs_bucket=config.gcs_bucket,
            delivery_date=delivery_date,
            table_name=dervied_table,
            project_id=config.project_id,
            dataset_id=config.dataset_id,
            vocab_version=constants.OMOP_TARGET_VOCAB_VERSION
        )


@task(trigger_rule="none_failed")
def final_cleanup(sites_to_process: list[tuple[str, str]]) -> None:
    """
    Perform final data cleanup here.
    """
    utils.logger.info("Executing final cleanup tasks")

    for site, delivery_date in sites_to_process:
        config = SiteConfig(site=site)
        run_id = TaskContext.get_run_id()

        try:
            bq.bq_log_running(site=site, delivery_date=delivery_date, run_id=run_id)

            # Generate final data delivery report
            report_data = omop.generate_report_json(site=site, delivery_date=delivery_date)
            validation.generate_delivery_report(report_data=report_data)

            # Create empty tables for OMOP files not provided in delivery
            omop.create_missing_omop_tables(
                project_id=config.project_id,
                dataset_id=config.dataset_id,
                omop_version=constants.OMOP_TARGET_CDM_VERSION
            )

            # Add record to cdm_source table in BigQuery, if not provided by site
            cdm_source_data = omop.generate_cdm_source_json(site=site, delivery_date=delivery_date)
            omop.populate_cdm_source(cdm_source_data=cdm_source_data)

            # Add completed log entry to BigQuery tracking table
            bq.bq_log_complete(site=site, delivery_date=delivery_date, run_id=run_id)
        except Exception as e:
            bq.bq_log_error(site=site, delivery_date=delivery_date, run_id=run_id, message=str(e))
            raise Exception(f"Unable to perform final cleanup: {e}") from e


@task(max_active_tis_per_dag=10, trigger_rule="none_failed", execution_timeout=timedelta(hours=3))
@log_task_execution()
def dqd(site_to_process: tuple[str, str]) -> None:
    site, delivery_date = site_to_process
    config = SiteConfig(site=site)

    utils.logger.info(f"Triggering DQD checks for {site} data delivered on {delivery_date}")

    gcs_artifact_path = os.path.join(
        config.gcs_bucket,
        delivery_date,
        constants.ArtifactPaths.DQD.value
    )

    # Execute DQD via Cloud Run Job
    analysis.run_dqd_job(
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        gcs_artifact_path=gcs_artifact_path,
        cdm_version=constants.OMOP_TARGET_CDM_VERSION,
        cdm_source_name=config.display_name,
        context=TaskContext.get_context()
    )

@task(max_active_tis_per_dag=10, trigger_rule="none_failed", execution_timeout=timedelta(hours=3))
@log_task_execution()
def achilles(site_to_process: tuple[str, str]) -> None:
    site, delivery_date = site_to_process
    config = SiteConfig(site=site)

    utils.logger.info(f"Triggering Achilles analyses for {site} data delivered on {delivery_date}")

    gcs_artifact_path = os.path.join(
        config.gcs_bucket,
        delivery_date,
        constants.ArtifactPaths.ACHILLES.value
    )

    # Execute Achilles via Cloud Run Job
    analysis.run_achilles_job(
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        gcs_artifact_path=gcs_artifact_path,
        cdm_version=constants.OMOP_TARGET_CDM_VERSION,
        cdm_source_name=config.display_name,
        context=TaskContext.get_context()
    )

@task(trigger_rule=TriggerRule.ALL_DONE, retries=0)
def log_done() -> None:
    # This final task will run regardless of previous task states.
    context = TaskContext.get_context()
    dag_run = context.get('dag_run')

    # Early return if dag_run is not available (shouldn't happen in normal operation)
    if dag_run is None:
        utils.logger.warning("No dag_run found in context. Skipping failure detection.")
        return

    run_id = dag_run.run_id
    task_instances = dag_run.get_task_instances()

    failures_detected = False

    # Check if any tasks failed
    for ti in task_instances:
        if "fail" in ti.state.lower():
            bq.bq_log_error(
                site="ALL SITES",
                delivery_date="ALL DELIVERIES",
                run_id=run_id,
                message=constants.PIPELINE_DAG_FAIL_MESSAGE
            )
            failures_detected = True

    # Check if the dag_run itself is in a failed state
    if "fail" in dag_run.state.lower():
        bq.bq_log_error(
            site="ALL SITES",
            delivery_date="ALL DELIVERIES",
            run_id=run_id,
            message=constants.PIPELINE_DAG_FAIL_MESSAGE
        )
        failures_detected = True

    # Raise an exception to fail this task (and thus the entire DAG)
    if failures_detected:
        utils.logger.error("Failures detected in DAG execution. Marking the DAG as failed.")
        raise AirflowException("DAG execution failed due to one or more task failures.")


# Define the DAG structure.
with dag:
    api_health_check = check_api_health()
    unprocessed_sites = id_sites_to_process()
    sites_exist = end_if_all_processed(unprocessed_sites)
    file_list = get_unprocessed_files(sites_to_process=unprocessed_sites)
    
    # Expand the processing tasks across the list of file configurations.
    process_files = convert_file.expand(file_config_dict=file_list)
    validate_files = validate_file.expand(file_config_dict=file_list)
    fix_data_file = normalize_file.expand(file_config_dict=file_list)
    upgrade_file = cdm_upgrade.expand(file_config_dict=file_list)

    # Vocab harmonization task group with 5 sequential steps
    with TaskGroup(group_id="vocab_harmonization", tooltip="Multi-step vocabulary harmonization process") as vocab_harmonization_group:
        # Each step is expanded across all file configs and executes in order
        vocab_step1_source_target = harmonize_vocab_source_target.expand(file_config_dict=file_list)
        vocab_step2_target_remap = harmonize_vocab_target_remap.expand(file_config_dict=file_list)
        vocab_step3_target_replacement = harmonize_vocab_target_replacement.expand(file_config_dict=file_list)
        vocab_step4_domain_check = harmonize_vocab_domain_check.expand(file_config_dict=file_list)
        vocab_step5_omop_etl = harmonize_vocab_omop_etl.expand(file_config_dict=file_list)
        vocab_step6_consolidate = harmonize_vocab_consolidate.expand(site_to_process=unprocessed_sites)
        vocab_step7_deduplicate = harmonize_vocab_primary_keys_dedup.expand(site_to_process=unprocessed_sites)

        # Chain the 6 vocabulary harmonization steps sequentially within the group
        vocab_step1_source_target >> vocab_step2_target_remap >> vocab_step3_target_replacement >> vocab_step4_domain_check >> vocab_step5_omop_etl >> vocab_step6_consolidate >> vocab_step7_deduplicate

    # BigQuery loading task group
    with TaskGroup(group_id="load_to_bigquery", tooltip="Load all data to BigQuery") as load_to_bigquery_group:
        # Prepare BigQuery by removing old tables
        clean_bq = prepare_bq.expand(site_to_process=unprocessed_sites)
        
        # Load harmonized vocabulary tables
        load_harmonized = load_harmonized_tables.expand(site_to_process=unprocessed_sites)
        
        # Load target vocabulary tables
        load_vocab = load_target_vocab.expand(site_to_process=unprocessed_sites)
        
        # Load remaining OMOP data files
        load_others = load_remaining.expand(file_config_dict=file_list)
        
        # Chain the loading steps sequentially within the group
        clean_bq >> load_harmonized >> load_vocab >> load_others

    # After files have been harmonized, populate derived data
    derived_data = derived_data_tables.expand(site_to_process=unprocessed_sites)
    cleanup = final_cleanup(sites_to_process=unprocessed_sites)

    # Run Data Quality Dashboard after loading data
    run_dqd = dqd.expand(site_to_process=unprocessed_sites)

    # Run Achilles analyses after loading data
    run_achilles = achilles.expand(site_to_process=unprocessed_sites)

    # Final log_done task runs regardless of task outcomes.
    all_done = log_done()
    
    # Set task dependencies
    api_health_check >> unprocessed_sites >> sites_exist >> file_list
    file_list >> process_files >> validate_files >> fix_data_file >> upgrade_file
    
    # Vocab harmonization task group executes after file upgrade
    upgrade_file >> vocab_harmonization_group
    
    # BigQuery loading task group executes after vocab harmonization
    vocab_harmonization_group >> load_to_bigquery_group
    
    # Continue with remaining tasks loading dataset
    load_to_bigquery_group >> derived_data >> cleanup

    # Run analytics tasks in parallel
    cleanup >> [run_dqd, run_achilles] >> all_done