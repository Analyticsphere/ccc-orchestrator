from datetime import timedelta

import airflow  # type: ignore
import dependencies.ehr.analysis as analysis
import dependencies.ehr.bq as bq
import dependencies.ehr.constants as constants
import dependencies.ehr.file_config as file_config
import dependencies.ehr.omop as omop
import dependencies.ehr.processing as processing
import dependencies.ehr.processing_jobs as processing_jobs
import dependencies.ehr.utils as utils
import dependencies.ehr.validation as validation
import dependencies.ehr.vocab as vocab
from airflow import DAG  # type: ignore
from airflow.decorators import task  # type: ignore
from airflow.exceptions import AirflowException  # type: ignore
from airflow.exceptions import AirflowSkipException  # type: ignore
from airflow.utils.dates import days_ago  # type: ignore
from airflow.utils.task_group import TaskGroup  # type: ignore
from airflow.utils.trigger_rule import TriggerRule  # type: ignore
from dependencies.ehr.dag_helpers import (SiteConfig, TaskContext,
                                          format_log_context,
                                          log_task_execution)
from dependencies.ehr.storage_backend import storage

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


@task(execution_timeout=timedelta(minutes=5))
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


@task(execution_timeout=timedelta(minutes=15))
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


@task.short_circuit(execution_timeout=timedelta(minutes=5))
def end_if_all_processed(unprocessed_sites: list[tuple[str, str]]) -> bool:
    """
    Returns False (which skips downstream tasks) if there are no sites to process.
    """
    if not unprocessed_sites:
        utils.logger.info("No unprocessed sites found. Skipping processing tasks.")
        return False
    return True


@task(execution_timeout=timedelta(minutes=30), trigger_rule="none_failed")
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


@task(max_active_tis_per_dag=24, trigger_rule="none_failed", execution_timeout=timedelta(hours=1))
@log_task_execution()
def convert_file(file_config_dict: dict) -> None:
    """
    Create optimized version of incoming EHR data file via Cloud Run Job.
    """
    fc = file_config.FileConfig.from_dict(config_dict=file_config_dict)
    config = SiteConfig(site=fc.site)

    processing_jobs.run_process_file_job(
        file_type=fc.file_delivery_format,
        gcs_file_path=fc.file_path,
        project_id=config.project_id,
        context=TaskContext.get_context(),
        site=fc.site,
        delivery_date=fc.delivery_date
    )


@task(max_active_tis_per_dag=24, trigger_rule="none_failed", execution_timeout=timedelta(minutes=30))
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


@task(max_active_tis_per_dag=24, trigger_rule="none_failed",execution_timeout=timedelta(hours=1))
@log_task_execution()
def normalize_file(file_config_dict: dict) -> None:
    """
    Standardize OMOP data file structure via Cloud Run Job.
    """
    fc = file_config.FileConfig.from_dict(config_dict=file_config_dict)
    config = SiteConfig(site=fc.site)

    processing_jobs.run_normalize_parquet_job(
        file_path=fc.file_path,
        cdm_version=fc.omop_version,
        date_format=fc.date_format,
        datetime_format=fc.datetime_format,
        project_id=config.project_id,
        context=TaskContext.get_context(),
        site=fc.site,
        delivery_date=fc.delivery_date,
        file=fc.table_name
    )


@task(max_active_tis_per_dag=24, trigger_rule="none_failed", execution_timeout=timedelta(hours=1))
@log_task_execution()
def cdm_upgrade(file_config_dict: dict) -> None:
    """
    Upgrade CDM version (currently supports 5.3 -> 5.4) via Cloud Run Job.
    """
    fc = file_config.FileConfig.from_dict(config_dict=file_config_dict)
    config = SiteConfig(site=fc.site)
    log_ctx = format_log_context(site=fc.site, delivery_date=fc.delivery_date, file=fc.table_name)

    if fc.omop_version == constants.OMOP_TARGET_CDM_VERSION:
        utils.logger.info(
            f"{log_ctx}CDM version ({fc.omop_version}) matches upgrade target "
            f"{constants.OMOP_TARGET_CDM_VERSION}; skipping upgrade"
        )
    else:
        processing_jobs.run_upgrade_cdm_job(
            file_path=fc.file_path,
            cdm_version=fc.omop_version,
            target_cdm_version=constants.OMOP_TARGET_CDM_VERSION,
            project_id=config.project_id,
            context=TaskContext.get_context(),
            site=fc.site,
            delivery_date=fc.delivery_date,
            file=fc.table_name
        )


@task(max_active_tis_per_dag=10, trigger_rule="none_failed", execution_timeout=timedelta(minutes=10))
@log_task_execution()
def populate_cdm_source_file(site_to_process: tuple[str, str]) -> None:
    """
    Populate cdm_source Parquet file with metadata if empty or non-existent.

    Ensures cdm_source.parquet exists with proper metadata before BigQuery loading.
    The Parquet file will be loaded to BigQuery through the normal data pipeline.
    """
    site, delivery_date = site_to_process

    cdm_source_data = omop.generate_cdm_source_json(site=site, delivery_date=delivery_date)
    omop.populate_cdm_source_file(cdm_source_data=cdm_source_data)


@task(max_active_tis_per_dag=24, trigger_rule="none_failed", execution_timeout=timedelta(minutes=constants.VOCAB_TIME_MIN))
@log_task_execution()
def harmonize_vocab_source_target(file_config_dict: dict) -> None:
    """
    Step 1: Execute source_target vocabulary harmonization via Cloud Run Job.
    """
    fc = file_config.FileConfig.from_dict(config_dict=file_config_dict)

    # Check if this table should be harmonized
    if not vocab.should_harmonize_table(table_name=fc.table_name):
        log_ctx = format_log_context(site=fc.site, delivery_date=fc.delivery_date, file=fc.table_name)
        utils.logger.info(f"{log_ctx}Table is not a clinical data table; skipping vocabulary harmonization")
        raise AirflowSkipException

    # Get configuration parameters
    config = SiteConfig(site=fc.site)

    # Execute source_target step via Cloud Run Job
    processing_jobs.run_harmonize_vocab_job(
        file_path=fc.file_path,
        site=fc.site,
        project_id=config.project_id,
        dataset_id=config.cdm_dataset_id,
        step=constants.SOURCE_TARGET,
        context=TaskContext.get_context(),
        delivery_date=fc.delivery_date
    )


@task(max_active_tis_per_dag=24, trigger_rule="none_failed", execution_timeout=timedelta(minutes=constants.VOCAB_TIME_MIN))
@log_task_execution()
def harmonize_vocab_target_remap(file_config_dict: dict) -> None:
    """
    Step 2: Execute target_remap vocabulary harmonization via Cloud Run Job.
    """
    fc = file_config.FileConfig.from_dict(config_dict=file_config_dict)

    # Check if this table should be harmonized
    if not vocab.should_harmonize_table(table_name=fc.table_name):
        raise AirflowSkipException

    # Get configuration parameters
    config = SiteConfig(site=fc.site)

    # Execute target_remap step via Cloud Run Job
    processing_jobs.run_harmonize_vocab_job(
        file_path=fc.file_path,
        site=fc.site,
        project_id=config.project_id,
        dataset_id=config.cdm_dataset_id,
        step=constants.TARGET_REMAP,
        context=TaskContext.get_context(),
        delivery_date=fc.delivery_date
    )


@task(max_active_tis_per_dag=24, trigger_rule="none_failed", execution_timeout=timedelta(minutes=constants.VOCAB_TIME_MIN))
@log_task_execution()
def harmonize_vocab_target_replacement(file_config_dict: dict) -> None:
    """
    Step 3: Execute target_replacement vocabulary harmonization via Cloud Run Job.
    """
    fc = file_config.FileConfig.from_dict(config_dict=file_config_dict)

    # Check if this table should be harmonized
    if not vocab.should_harmonize_table(table_name=fc.table_name):
        raise AirflowSkipException

    # Get configuration parameters
    config = SiteConfig(site=fc.site)

    # Execute target_replacement step via Cloud Run Job
    processing_jobs.run_harmonize_vocab_job(
        file_path=fc.file_path,
        site=fc.site,
        project_id=config.project_id,
        dataset_id=config.cdm_dataset_id,
        step=constants.TARGET_REPLACEMENT,
        context=TaskContext.get_context(),
        delivery_date=fc.delivery_date
    )


@task(max_active_tis_per_dag=24, trigger_rule="none_failed", execution_timeout=timedelta(minutes=constants.VOCAB_TIME_MIN))
@log_task_execution()
def harmonize_vocab_domain_check(file_config_dict: dict) -> None:
    """
    Step 4: Execute domain_check vocabulary harmonization via Cloud Run Job.
    """
    fc = file_config.FileConfig.from_dict(config_dict=file_config_dict)

    # Check if this table should be harmonized
    if not vocab.should_harmonize_table(table_name=fc.table_name):
        raise AirflowSkipException

    # Get configuration parameters
    config = SiteConfig(site=fc.site)

    # Execute domain_check step via Cloud Run Job
    processing_jobs.run_harmonize_vocab_job(
        file_path=fc.file_path,
        site=fc.site,
        project_id=config.project_id,
        dataset_id=config.cdm_dataset_id,
        step=constants.DOMAIN_CHECK,
        context=TaskContext.get_context(),
        delivery_date=fc.delivery_date
    )


@task(max_active_tis_per_dag=24, trigger_rule="none_failed", execution_timeout=timedelta(minutes=constants.VOCAB_TIME_MIN))
@log_task_execution()
def harmonize_vocab_omop_etl(file_config_dict: dict) -> None:
    """
    Step 5: Execute omop_etl vocabulary harmonization via Cloud Run Job.
    """
    fc = file_config.FileConfig.from_dict(config_dict=file_config_dict)

    # Check if this table should be harmonized
    if not vocab.should_harmonize_table(table_name=fc.table_name):
        raise AirflowSkipException

    # Get configuration parameters
    config = SiteConfig(site=fc.site)

    # Execute omop_etl step via Cloud Run Job
    processing_jobs.run_harmonize_vocab_job(
        file_path=fc.file_path,
        site=fc.site,
        project_id=config.project_id,
        dataset_id=config.cdm_dataset_id,
        step=constants.OMOP_ETL,
        context=TaskContext.get_context(),
        delivery_date=fc.delivery_date
    )


@task(max_active_tis_per_dag=24, trigger_rule="none_failed", execution_timeout=timedelta(minutes=constants.VOCAB_TIME_MIN))
@log_task_execution()
def harmonize_vocab_consolidate(site_to_process: tuple[str, str]) -> None:
    """
    Step 6: Combine and deduplicate ETL files after vocabulary harmonization via Cloud Run Job.
    This occurs once per site; not per file
    """
    site, delivery_date = site_to_process
    config = SiteConfig(site=site)

    # Execute consolidation step via Cloud Run Job
    processing_jobs.run_harmonize_vocab_job(
        file_path=storage.get_uri(f"{config.gcs_bucket}/{delivery_date}/dummy_value_for_consolidation"),
        site=site,
        project_id=config.project_id,
        dataset_id=config.cdm_dataset_id,
        step=constants.CONSOLIDATE_ETL,
        context=TaskContext.get_context(),
        delivery_date=delivery_date
    )


@task(max_active_tis_per_dag=24, trigger_rule="none_failed", execution_timeout=timedelta(minutes=constants.VOCAB_TIME_MIN))
@log_task_execution()
def harmonize_vocab_discover_tables(site_to_process: tuple[str, str]) -> list[dict]:
    """
    Step 7: Discover all tables that need primary key deduplication via Cloud Run Job.
    This occurs once per site and returns a list of table configurations for parallel processing.
    """
    site, delivery_date = site_to_process
    config = SiteConfig(site=site)

    # Discover all tables that need deduplication via Cloud Run Job
    table_configs = processing_jobs.run_discover_tables_job(
        file_path=storage.get_uri(f"{config.gcs_bucket}/{delivery_date}/dummy_value_for_discovery"),
        site=site,
        project_id=config.project_id,
        dataset_id=config.cdm_dataset_id,
        gcs_bucket=config.gcs_bucket,
        delivery_date=delivery_date,
        context=TaskContext.get_context()
    )

    return table_configs


@task(trigger_rule="none_failed", execution_timeout=timedelta(minutes=15))
def flatten_table_configs(table_configs_by_site: list[list[dict]]) -> list[dict]:
    """
    Flatten the list of table configs from all sites into a single list.
    Each site returns a list of table configs, this combines them all.
    """
    flattened = []
    for site_tables in table_configs_by_site:
        if site_tables:  # Skip empty lists
            flattened.extend(site_tables)

    utils.logger.info(f"Flattened {len(flattened)} table(s) from {len(table_configs_by_site)} site(s)")
    return flattened


@task(max_active_tis_per_dag=24, trigger_rule="none_failed", execution_timeout=timedelta(minutes=constants.VOCAB_TIME_MIN))
@log_task_execution()
def harmonize_vocab_deduplicate_table(table_config: dict) -> None:
    """
    Step 8: Deduplicate primary keys for a single table via Cloud Run Job.
    This runs in parallel for each table that needs deduplication.
    """
    # Deduplicate primary keys for this specific table via Cloud Run Job
    processing_jobs.run_deduplicate_table_job(
        table_config=table_config,
        context=TaskContext.get_context()
    )


@task(execution_timeout=timedelta(minutes=5), trigger_rule="none_failed")
def create_derived_table_configs(sites_to_process: list[tuple[str, str]]) -> list[dict]:
    """
    Create a configuration dictionary for each site-table combination.

    For example, if we have 2 sites and 3 derived tables, this will create 6 configs
    (one for each combination), allowing for parallel processing of each table.
    """
    configs = []
    for site, delivery_date in sites_to_process:
        for table_name in constants.DERIVED_DATA_TABLES:
            configs.append({
                'site': site,
                'delivery_date': delivery_date,
                'table_name': table_name
            })
    return configs


@task(max_active_tis_per_dag=10, trigger_rule="none_failed", execution_timeout=timedelta(hours=1))
@log_task_execution()
def generate_single_derived_table(derived_config: dict) -> None:
    """
    Generate a single derived table from HARMONIZED data via Cloud Run Job.

    This task is called AFTER vocabulary harmonization is complete and reads from harmonized
    Parquet files in omop_etl/. The derived table is written to derived_files/ and will be
    loaded to BigQuery in the load_to_bigquery task group.

    This task is expanded per site-table combination, allowing parallel processing.
    For example: 2 sites × 3 tables = 6 parallel tasks.
    """
    site = derived_config['site']
    delivery_date = derived_config['delivery_date']
    table_name = derived_config['table_name']

    config = SiteConfig(site=site)

    processing_jobs.run_generate_derived_table_job(
        site=site,
        gcs_bucket=config.gcs_bucket,
        delivery_date=delivery_date,
        table_name=table_name,
        vocab_version=constants.OMOP_TARGET_VOCAB_VERSION,
        project_id=config.project_id,
        context=TaskContext.get_context()
    )

@task(max_active_tis_per_dag=10, trigger_rule="none_failed", execution_timeout=timedelta(minutes=30))
@log_task_execution()
def prepare_bq(site_to_process: tuple[str, str]) -> None:
    """
    Deletes files and tables from previous pipeline runs.
    """
    site, delivery_date = site_to_process
    config = SiteConfig(site=site)

    # Delete all tables within the BigQuery dataset.
    bq.prep_dataset(project_id=config.project_id, dataset_id=config.cdm_dataset_id, site=site, delivery_date=delivery_date)


@task(max_active_tis_per_dag=10, trigger_rule="none_failed", execution_timeout=timedelta(minutes=30))
@log_task_execution()
def load_harmonized_tables(site_to_process: tuple[str, str]) -> None:
    """
    Load all harmonized vocabulary tables to BigQuery.

    Skips if no harmonized tables are available (i.e., no clinical tables in delivery).
    """
    site, delivery_date = site_to_process
    config = SiteConfig(site=site)
    log_ctx = utils.format_log_context(site=site, delivery_date=delivery_date)

    try:
        bq.load_harmonized_tables_to_bq(
            gcs_bucket=config.gcs_bucket,
            delivery_date=delivery_date,
            project_id=config.project_id,
            dataset_id=config.cdm_dataset_id,
            site=site
        )
    except Exception as e:
        # Check if error is due to no harmonized files existing
        error_msg = str(e)
        if "No table directories found" in error_msg or "No harmonized tables found" in error_msg:
            utils.logger.info(f"{log_ctx}No harmonized tables found; skipping load (no clinical tables in delivery)")
            raise AirflowSkipException(f"No harmonized tables to load for {site}")
        else:
            # Re-raise other errors
            raise
    

@task(max_active_tis_per_dag=10, trigger_rule="none_failed", execution_timeout=timedelta(minutes=30))
@log_task_execution(skip_running_log=True)
def load_target_vocab(site_to_process: tuple[str, str]) -> None:
    """
    Load all target vocabulary tables to BigQuery
    If the site's overwrite_site_vocab_with_standard is False, the site's vocab tables will overwrite default tables loaded by this task
    """
    site, delivery_date = site_to_process
    config = SiteConfig(site=site)

    if config.overwrite_site_vocab_with_standard:
        for vocab_table in constants.VOCABULARY_TABLES:
            vocab.load_vocabulary_table_gcs_to_bq(
                vocab_version=constants.OMOP_TARGET_VOCAB_VERSION,
                table_file_name=vocab_table,
                project_id=config.project_id,
                dataset_id=config.cdm_dataset_id,
                site=site,
                delivery_date=delivery_date
            )

@task(max_active_tis_per_dag=24, trigger_rule="none_failed", execution_timeout=timedelta(minutes=30))
@log_task_execution()
def load_remaining(file_config_dict: dict) -> None:
    """
    Load OMOP data file to BigQuery table.
    """
    fc = file_config.FileConfig.from_dict(config_dict=file_config_dict)
    config = SiteConfig(site=fc.site)
    log_ctx = format_log_context(site=fc.site, delivery_date=fc.delivery_date, file=fc.table_name)

    # Don't load standard vocabulary files if using site-specific vocabulary
    if config.overwrite_site_vocab_with_standard and fc.table_name in constants.VOCABULARY_TABLES:
        utils.logger.info(f"{log_ctx}Skipping vocabulary table load; using site-specific vocabulary")
        raise AirflowSkipException

    # Also skip vocab harmonized tables since they were already loaded in harmonize_vocab task
    elif fc.table_name in constants.VOCAB_HARMONIZED_TABLES:
        utils.logger.info(f"{log_ctx}Skipping load; already loaded in harmonize_vocab task")
        raise AirflowSkipException
    elif fc.table_name == "cdm_source":
        utils.logger.info(f"{log_ctx}Skipping load; will be handled in cleanup task")
        raise AirflowSkipException
    else:
        bq.load_individual_parquet_to_bq(
            file_path=fc.file_path,
            project_id=config.project_id,
            dataset_id=config.cdm_dataset_id,
            table_name=fc.table_name,
            write_type=constants.BQWriteTypes.PROCESSED_FILE,
            site=fc.site,
            delivery_date=fc.delivery_date
        )

@task(max_active_tis_per_dag=10, trigger_rule="none_failed", execution_timeout=timedelta(minutes=30))
@log_task_execution()
def load_derived_tables(site_to_process: tuple[str, str]) -> None:
    """
    Load derived tables from derived_files/ to BigQuery.

    This task discovers all derived table Parquet files in artifacts/derived_files/
    and loads them to their corresponding BigQuery tables.
    """
    site, delivery_date = site_to_process
    config = SiteConfig(site=site)

    omop.load_derived_tables_to_bigquery(
        gcs_bucket=config.gcs_bucket,
        delivery_date=delivery_date,
        project_id=config.project_id,
        dataset_id=config.cdm_dataset_id,
        site=site
    )

@task(trigger_rule="none_failed", execution_timeout=timedelta(minutes=30))
def cleanup(sites_to_process: list[tuple[str, str]]) -> None:
    """
    Perform CDM setup and cleanup tasks after BigQuery loading.
    Prepares the CDM dataset for DQD and Achilles analyses.
    """
    utils.logger.info("Executing CDM setup and cleanup tasks")

    for site, delivery_date in sites_to_process:
        config = SiteConfig(site=site)
        run_id = TaskContext.get_run_id()
        log_ctx = format_log_context(site=site, delivery_date=delivery_date)

        try:
            bq.bq_log_running(site=site, delivery_date=delivery_date, run_id=run_id)

            # Create empty tables for OMOP files not provided in delivery
            omop.create_missing_omop_tables(
                project_id=config.project_id,
                dataset_id=config.cdm_dataset_id,
                omop_version=constants.OMOP_TARGET_CDM_VERSION,
                site=site,
                delivery_date=delivery_date
            )

            # Load cdm_source file to BigQuery (guaranteed to exist after populate_cdm_source_file task)
            cdm_source_file_path = f"{config.gcs_bucket}/{delivery_date}/artifacts/converted_files/cdm_source.parquet"
            bq.load_individual_parquet_to_bq(
                file_path=cdm_source_file_path,
                project_id=config.project_id,
                dataset_id=config.cdm_dataset_id,
                table_name="cdm_source",
                write_type=constants.BQWriteTypes.PROCESSED_FILE,
                site=site,
                delivery_date=delivery_date
            )

            utils.logger.info(f"{log_ctx}CDM cleanup and setup completed successfully")
        except Exception as e:
            bq.bq_log_error(site=site, delivery_date=delivery_date, run_id=run_id, message=str(e))
            raise Exception(f"{log_ctx}Unable to perform CDM cleanup: {e}") from e


@task(max_active_tis_per_dag=10, trigger_rule="none_failed", execution_timeout=timedelta(minutes=60))
@log_task_execution()
def generate_report_csv(site_to_process: tuple[str, str]) -> None:
    """
    Generate delivery report CSV file with processing metadata and statistics.

    This CSV file serves as the data source for visual reporting and dashboards.
    The report includes metadata, row counts, vocabulary breakdowns, and type concept distributions.
    """
    site, delivery_date = site_to_process
    config = SiteConfig(site=site)
    log_ctx = format_log_context(site=site, delivery_date=delivery_date)

    utils.logger.info(f"{log_ctx}Generating delivery report CSV")

    # Execute report generation via Cloud Run Job
    processing_jobs.run_generate_report_csv_job(
        site=site,
        gcs_bucket=config.gcs_bucket,
        delivery_date=delivery_date,
        site_display_name=config.display_name,
        file_delivery_format=config.file_delivery_format,
        delivered_cdm_version=config.omop_version,
        target_vocabulary_version=constants.OMOP_TARGET_VOCAB_VERSION,
        target_cdm_version=constants.OMOP_TARGET_CDM_VERSION,
        project_id=config.project_id,
        context=TaskContext.get_context()
    )


@task(max_active_tis_per_dag=10, trigger_rule="none_failed", execution_timeout=timedelta(hours=3))
@log_task_execution()
def dqd(site_to_process: tuple[str, str]) -> None:
    """Execute the Data Quality Dashboard (DQD) checks via Cloud Run Job."""
    site, delivery_date = site_to_process
    config = SiteConfig(site=site)
    log_ctx = format_log_context(site=site, delivery_date=delivery_date)

    utils.logger.info(f"{log_ctx}Triggering DQD (Data Quality Dashboard) checks")

    gcs_artifact_path = f"{config.gcs_bucket}/{delivery_date}/{constants.ArtifactPaths.DQD.value}"

    # Execute DQD via Cloud Run Job
    analysis.run_dqd_job(
        project_id=config.project_id,
        dataset_id=config.cdm_dataset_id,
        analytics_dataset_id=config.analytics_dataset_id,
        gcs_artifact_path=gcs_artifact_path,
        cdm_version=constants.OMOP_TARGET_CDM_VERSION,
        cdm_source_name=config.display_name,
        context=TaskContext.get_context(),
        site=site,
        delivery_date=delivery_date
    )

@task(max_active_tis_per_dag=10, trigger_rule="none_failed", execution_timeout=timedelta(hours=3))
@log_task_execution()
def achilles(site_to_process: tuple[str, str]) -> None:
    

    site, delivery_date = site_to_process
    config = SiteConfig(site=site)
    log_ctx = format_log_context(site=site, delivery_date=delivery_date)

    utils.logger.info(f"{log_ctx}Triggering Achilles analyses")

    gcs_artifact_path = f"{config.gcs_bucket}/{delivery_date}/{constants.ArtifactPaths.ACHILLES.value}"

    # Execute Achilles via Cloud Run Job
    # Achilles reads CDM data from cdm_dataset_id and writes results to analytics_dataset_id
    analysis.run_achilles_job(
        project_id=config.project_id,
        dataset_id=config.cdm_dataset_id,
        analytics_dataset_id=config.analytics_dataset_id,
        gcs_artifact_path=gcs_artifact_path,
        cdm_version=constants.OMOP_TARGET_CDM_VERSION,
        cdm_source_name=config.display_name,
        context=TaskContext.get_context(),
        site=site,
        delivery_date=delivery_date
    )

@task(max_active_tis_per_dag=10, trigger_rule="none_failed", execution_timeout=timedelta(minutes=30))
@log_task_execution()
def atlas_results_tables(site_to_process: tuple[str, str]) -> None:
    """Run Atlas results tables creation via API endpoint."""
    site, delivery_date = site_to_process
    config = SiteConfig(site=site)
    log_ctx = format_log_context(site=site, delivery_date=delivery_date)

    utils.logger.info(f"{log_ctx}Creating Atlas results tables")

    # Create Atlas results tables via API endpoint
    analysis.create_atlas_results_tables(
        project_id=config.project_id,
        cdm_dataset_id=config.cdm_dataset_id,
        analytics_dataset_id=config.analytics_dataset_id,
        site=site,
        delivery_date=delivery_date
    )

@task(max_active_tis_per_dag=10, trigger_rule="none_failed", execution_timeout=timedelta(minutes=30))
@log_task_execution()
def generate_delivery_report(site_to_process: tuple[str, str]) -> None:
    """Generate interactive HTML delivery report via API endpoint."""
    site, delivery_date = site_to_process
    config = SiteConfig(site=site)
    log_ctx = format_log_context(site=site, delivery_date=delivery_date)

    utils.logger.info(f"{log_ctx}Generating interactive HTML delivery report")

    # Generate delivery report via API endpoint
    analysis.generate_delivery_report(
        gcs_bucket=config.gcs_bucket,
        delivery_date=delivery_date,
        site=site
    )

@task(trigger_rule="none_failed", execution_timeout=timedelta(minutes=10))
def mark_delivery_complete(sites_to_process: list[tuple[str, str]]) -> None:
    """
    Mark data deliveries as complete after all analyses (DQD, Achilles, Atlas) finish successfully.
    This task logs completion status to BigQuery tracking table.
    """
    utils.logger.info("Marking deliveries as complete after all pipeline tasks and analyses finished")

    for site, delivery_date in sites_to_process:
        run_id = TaskContext.get_run_id()

        try:
            # Add completed log entry to BigQuery tracking table
            bq.bq_log_complete(site=site, delivery_date=delivery_date, run_id=run_id)
            utils.logger.info(f"Marked delivery complete for {site} delivery {delivery_date}")
        except Exception as e:
            bq.bq_log_error(site=site, delivery_date=delivery_date, run_id=run_id, message=str(e))
            raise Exception(f"Unable to mark delivery as complete: {e}") from e

@task(trigger_rule=TriggerRule.ALL_DONE, retries=0, execution_timeout=timedelta(minutes=10))
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

    # Vocab harmonization task group with 8 steps (plus a flattening helper)
    with TaskGroup(group_id="vocab_harmonization", tooltip="Multi-step vocabulary harmonization process") as vocab_harmonization_group:
        # Each step is expanded across all file configs and executes in order
        vocab_step1_source_target = harmonize_vocab_source_target.expand(file_config_dict=file_list)
        vocab_step2_target_remap = harmonize_vocab_target_remap.expand(file_config_dict=file_list)
        vocab_step3_target_replacement = harmonize_vocab_target_replacement.expand(file_config_dict=file_list)
        vocab_step4_domain_check = harmonize_vocab_domain_check.expand(file_config_dict=file_list)
        vocab_step5_omop_etl = harmonize_vocab_omop_etl.expand(file_config_dict=file_list)
        vocab_step6_consolidate = harmonize_vocab_consolidate.expand(site_to_process=unprocessed_sites)

        # Step 7: Discover tables for deduplication (per site)
        vocab_step7_discover = harmonize_vocab_discover_tables.expand(site_to_process=unprocessed_sites)

        # Step 7b: Flatten all table configs from all sites into a single list
        vocab_step7b_flatten = flatten_table_configs(table_configs_by_site=vocab_step7_discover)

        # Step 8: Deduplicate each table in parallel (mapped per table)
        vocab_step8_deduplicate = harmonize_vocab_deduplicate_table.expand(table_config=vocab_step7b_flatten)

        # Chain the 8 vocabulary harmonization steps sequentially within the group
        vocab_step1_source_target >> vocab_step2_target_remap >> vocab_step3_target_replacement >> vocab_step4_domain_check >> vocab_step5_omop_etl >> vocab_step6_consolidate >> vocab_step7_discover >> vocab_step7b_flatten >> vocab_step8_deduplicate

    # Derived table generation task group
    with TaskGroup(group_id="generate_derived_tables", tooltip="Generate derived tables from harmonized data") as generate_derived_tables_group:
        # Create configs for each site-table combination (e.g., 2 sites × 3 tables = 6 configs)
        derived_configs = create_derived_table_configs(sites_to_process=unprocessed_sites)

        # Generate each derived table in parallel (one task per site-table combination)
        gen_derived = generate_single_derived_table.expand(derived_config=derived_configs)

        # Chain the steps within the group
        derived_configs >> gen_derived

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

        # Load derived tables from derived_files/
        load_derived = load_derived_tables.expand(site_to_process=unprocessed_sites)

        # Chain the loading steps sequentially within the group
        clean_bq >> load_harmonized >> load_vocab >> load_others >> load_derived

    clean = cleanup(sites_to_process=unprocessed_sites)

    # Generate delivery report CSV file
    run_report = generate_report_csv.expand(site_to_process=unprocessed_sites)

    # Run Data Quality Dashboard after loading data
    run_dqd = dqd.expand(site_to_process=unprocessed_sites)

    # Run Achilles analyses after loading data
    run_achilles = achilles.expand(site_to_process=unprocessed_sites)

    # Create Atlas results tables after analyses complete
    create_atlas_tables = atlas_results_tables.expand(site_to_process=unprocessed_sites)

    # Generate interactive HTML delivery report after analyses complete
    gen_delivery_report = generate_delivery_report.expand(site_to_process=unprocessed_sites)

    # Mark deliveries as complete after all analyses finish
    mark_complete = mark_delivery_complete(sites_to_process=unprocessed_sites)

    # Final log_done task runs regardless of task outcomes.
    all_done = log_done()

    # Expand the new populate_cdm_source_file task
    populate_cdm_source_files = populate_cdm_source_file.expand(site_to_process=unprocessed_sites)

    # Set task dependencies
    api_health_check >> unprocessed_sites >> sites_exist >> file_list
    file_list >> process_files >> validate_files >> fix_data_file >> upgrade_file

    # Populate cdm_source file after upgrade, before vocab harmonization
    upgrade_file >> populate_cdm_source_files >> vocab_harmonization_group

    # Generate derived tables from harmonized data (AFTER vocab harmonization, BEFORE BQ load)
    vocab_harmonization_group >> generate_derived_tables_group

    # BigQuery loading task group executes after derived tables are generated
    generate_derived_tables_group >> load_to_bigquery_group

    # Continue with remaining tasks after loading dataset
    load_to_bigquery_group >> clean

    # Generate report CSV, then run analytics tasks in parallel
    clean >> run_report >> [run_dqd, run_achilles]

    # After analytics complete, create Atlas tables and generate delivery report in parallel
    [run_dqd, run_achilles] >> create_atlas_tables >> mark_complete
    [run_dqd, run_achilles] >> gen_delivery_report >> mark_complete

    # Final logging
    mark_complete >> all_done