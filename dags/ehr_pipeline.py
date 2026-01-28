from datetime import timedelta

import airflow  # type: ignore
import dependencies.ehr.analysis as analysis
import dependencies.ehr.bq as bq
import dependencies.ehr.constants as constants
import dependencies.ehr.utils as utils
import dependencies.ehr.vocab as vocab
from airflow import DAG  # type: ignore
from airflow.decorators import task  # type: ignore
from airflow.utils.dates import days_ago  # type: ignore
from dependencies.ehr.dag_helpers import (SiteConfig, TaskContext,
                                          format_log_context,
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
    description='Simplified pipeline for testing PASS integration',
    start_date=days_ago(1)
)


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


@task(max_active_tis_per_dag=10, trigger_rule="none_failed", execution_timeout=timedelta(hours=3))
@log_task_execution()
def pass_analysis(site_to_process: tuple[str, str]) -> None:
    """Execute PASS (Profile of Analytic Suitability Score) analysis via Cloud Run Job."""
    site, delivery_date = site_to_process
    config = SiteConfig(site=site)
    log_ctx = format_log_context(site=site, delivery_date=delivery_date)

    utils.logger.info(f"{log_ctx}Triggering PASS (Profile of Analytic Suitability Score) analysis")

    gcs_artifact_path = f"{config.gcs_bucket}/{delivery_date}/{constants.ArtifactPaths.PASS_ANALYSIS.value}"

    # Execute PASS via Cloud Run Job
    # PASS reads CDM data from cdm_dataset_id to evaluate data quality across six dimensions
    analysis.run_pass_job(
        project_id=config.project_id,
        dataset_id=config.cdm_dataset_id,
        gcs_artifact_path=gcs_artifact_path,
        context=TaskContext.get_context(),
        site=site,
        delivery_date=delivery_date
    )


# Define the DAG structure
with dag:
    # Get list of sites to process
    unprocessed_sites = id_sites_to_process()

    # Short circuit if no sites need processing
    sites_exist = end_if_all_processed(unprocessed_sites)

    # Run PASS analysis on each site
    run_pass = pass_analysis.expand(site_to_process=unprocessed_sites)

    # Set task dependencies
    unprocessed_sites >> sites_exist >> run_pass
