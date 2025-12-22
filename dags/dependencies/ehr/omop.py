from datetime import datetime

from dependencies.ehr import constants, utils
from dependencies.ehr.dag_helpers import SiteConfig, format_log_context


def generate_report_json(site: str, delivery_date: str) -> dict:
    """Generate report JSON data for the delivery report."""
    config = SiteConfig(site=site)

    report_data = {
        "site": site,
        "bucket": config.gcs_bucket,
        "delivery_date": delivery_date,
        "site_display_name": config.display_name,
        "file_delivery_format": config.file_delivery_format,
        "delivered_cdm_version": config.omop_version,
        "target_vocabulary_version": constants.OMOP_TARGET_VOCAB_VERSION,
        "target_cdm_version": constants.OMOP_TARGET_CDM_VERSION,
    }

    return report_data

def generate_cdm_source_json(site: str, delivery_date: str) -> dict:
    """Generate JSON data needed to populate a blank cdm_source table."""
    config = SiteConfig(site=site)

    cdm_source = {
        "cdm_source_name": config.display_name,
        "cdm_source_abbreviation": site,
        "cdm_holder": "NIH/NCI Connect for Cancer Prevention Study",
        "source_description": f"Electronic Health Record (EHR) data from {site}",
        "source_documentation_reference": "",
        "cdm_etl_reference": "",
        "source_release_date": delivery_date,
        "cdm_release_date": datetime.today().strftime('%Y-%m-%d'),
        "cdm_version": config.omop_version,
        "bucket": config.gcs_bucket,
        "project_id": config.project_id,
        "dataset_id": config.cdm_dataset_id
    }

    return cdm_source

def create_missing_omop_tables(project_id: str, dataset_id: str, omop_version: str, site: str = None, delivery_date: str = None) -> None:
    """Create any missing OMOP CDM tables in BigQuery dataset."""
    
    log_ctx = format_log_context(site=site, delivery_date=delivery_date)
    utils.logger.info(f"{log_ctx}Creating missing OMOP CDM {omop_version} tables in: {project_id}.{dataset_id}")

    utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="create_missing_tables",
        json_data={
            "omop_version": omop_version,
            "project_id": project_id,
            "dataset_id": dataset_id
        },
        site=site,
        delivery_date=delivery_date
    )

def generate_derived_table_from_harmonized(site: str, bucket: str, delivery_date: str, table_name: str, vocab_version: str) -> None:
    """Generate a derived data table from HARMONIZED data (post-vocabulary harmonization)."""
    
    log_ctx = format_log_context(site=site, delivery_date=delivery_date, file=table_name)
    utils.logger.info(f"{log_ctx}Generating derived table from harmonized data")

    utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="generate_derived_tables_from_harmonized",
        json_data={
            "site": site,
            "bucket": bucket,
            "delivery_date": delivery_date,
            "table_name": table_name,
            "vocab_version": vocab_version
        },
        site=site,
        delivery_date=delivery_date,
        file=table_name
    )

def load_derived_tables_to_bigquery(gcs_bucket: str, delivery_date: str, project_id: str, dataset_id: str, site: str = None) -> None:
    """Load all derived table Parquet files from artifacts/derived_files/ to BigQuery."""
    
    log_ctx = format_log_context(site=site, delivery_date=delivery_date)
    utils.logger.info(f"{log_ctx}Loading derived tables to BigQuery: {project_id}.{dataset_id}")

    utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="load_derived_tables_to_bq",
        json_data={
            "bucket": gcs_bucket,
            "delivery_date": delivery_date,
            "project_id": project_id,
            "dataset_id": dataset_id
        },
        site=site,
        delivery_date=delivery_date
    )

def populate_cdm_source_file(cdm_source_data: dict) -> None:
    """
    Checks if site delivered cdm_source file and populates it with metadata if needed.

    Args:
        cdm_source_data: Dictionary containing CDM source metadata
    """
    

    site = cdm_source_data.get('cdm_source_abbreviation')
    delivery_date = cdm_source_data.get('source_release_date')
    log_ctx = format_log_context(site=site, delivery_date=delivery_date, file='cdm_source')

    utils.logger.info(f"{log_ctx}Populating cdm_source metadata file")

    utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="populate_cdm_source_file",
        json_data=cdm_source_data,
        site=site,
        delivery_date=delivery_date,
        file='cdm_source'
    )

def upgrade_cdm(file_path: str, cdm_version: str, target_cdm_version: str, site: str = None, delivery_date: str = None, file: str = None) -> None:
    """Upgrade CDM version of the file (e.g., from 5.3 to 5.4)"""
    
    log_ctx = format_log_context(site=site, delivery_date=delivery_date, file=file)
    utils.logger.info(f"{log_ctx}Upgrading CDM version: {cdm_version} → {target_cdm_version}")

    utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="upgrade_cdm",
        json_data={
            "file_path": file_path,
            "omop_version": cdm_version,
            "target_omop_version": target_cdm_version
        },
        site=site,
        delivery_date=delivery_date,
        file=file
    )
