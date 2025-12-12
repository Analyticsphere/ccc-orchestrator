from datetime import datetime

from dependencies.ehr import constants, utils
from dependencies.ehr.dag_helpers import SiteConfig


def generate_report_json(site: str, delivery_date: str) -> dict:
    # Generate final data delivery report
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
    config = SiteConfig(site=site)

    # Create JSON with data needed to populate a blank cdm_source table
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

def create_missing_omop_tables(project_id: str, dataset_id: str, omop_version: str) -> None:
    utils.logger.info(f"Creating any missing OMOP tables in {project_id}.{dataset_id}")
    
    utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="create_missing_tables",
        json_data={
            "omop_version": omop_version,
            "project_id": project_id,
            "dataset_id": dataset_id
        }
    )

def generate_derived_table_from_harmonized(site: str, bucket: str, delivery_date: str, table_name: str, vocab_version: str) -> None:
    """
    Generate a derived data table from HARMONIZED data (post-vocabulary harmonization).

    This function calls the file processor endpoint that generates derived tables using DuckDB
    from harmonized Parquet files. The derived table is written to artifacts/derived_files/
    and will be loaded to BigQuery in a separate step.
    """
    utils.logger.info(f"Generating derived data table {table_name} from harmonized data for {delivery_date} delivery from {site}")

    utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="generate_derived_tables_from_harmonized",
        json_data={
            "site": site,
            "bucket": bucket,
            "delivery_date": delivery_date,
            "table_name": table_name,
            "vocab_version": vocab_version
        }
    )

def load_derived_tables_to_bigquery(gcs_bucket: str, delivery_date: str, project_id: str, dataset_id: str) -> None:
    """
    Load all derived table Parquet files from artifacts/derived_files/ to BigQuery.

    This function discovers all derived table files in the derived_files directory and
    loads them to their corresponding BigQuery tables.
    """
    utils.logger.info(f"Loading derived tables from artifacts/derived_files/ to BigQuery for {delivery_date} delivery")

    utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="load_derived_tables_to_bq",
        json_data={
            "bucket": gcs_bucket,
            "delivery_date": delivery_date,
            "project_id": project_id,
            "dataset_id": dataset_id
        }
    )

def populate_cdm_source_file(cdm_source_data: dict) -> None:
    """
    Checks if site delivered cdm_source file and populates it with metadata if needed.
    """
    utils.logger.info(
        f"Checking cdm_source file for {cdm_source_data['source_release_date']} "
        f"delivery from {cdm_source_data['cdm_source_abbreviation']}"
    )

    utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="populate_cdm_source_file",
        json_data=cdm_source_data
    )

def upgrade_cdm(file_path: str, cdm_version: str, target_cdm_version: str) -> None:
    """
    Upgrade CDM version of the file (e.g., from 5.3 to 5.4)
    """
    utils.logger.info(f"Upgrading CDM version {cdm_version} of file gs://{file_path} to {target_cdm_version}")

    utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="upgrade_cdm",
        json_data={
            "file_path": file_path,
            "omop_version": cdm_version,
            "target_omop_version": target_cdm_version
        }
    )
