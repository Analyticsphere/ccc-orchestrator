from datetime import datetime

from dependencies.ehr import constants, utils


def generate_report_json(site: str, delivery_date: str) -> dict:
    # Generate final data delivery report
    report_data = {
        "site": site,
        "gcs_bucket": utils.get_site_bucket(site),
        "delivery_date": delivery_date,
        "site_display_name": utils.get_site_config_file()[constants.FileConfig.SITE.value][site][constants.FileConfig.DISPLAY_NAME.value],
        "file_delivery_format": utils.get_site_config_file()[constants.FileConfig.SITE.value][site][constants.FileConfig.FILE_DELIVERY_FORMAT.value],
        "delivered_cdm_version": utils.get_site_config_file()[constants.FileConfig.SITE.value][site][constants.FileConfig.OMOP_VERSION.value],
        "target_vocabulary_version": constants.OMOP_TARGET_VOCAB_VERSION,
        "target_cdm_version": constants.OMOP_TARGET_CDM_VERSION,
    }

    return report_data

def generate_cdm_source_json(site: str, delivery_date: str) -> dict:
    project_id = utils.get_site_config_file()[constants.FileConfig.SITE.value][site][constants.FileConfig.PROJECT_ID.value]
    dataset_id = utils.get_site_config_file()[constants.FileConfig.SITE.value][site][constants.FileConfig.BQ_DATASET.value]

    # Create JSON with data needed to populate a blank cdm_source table
    cdm_source = {
        "cdm_source_name": utils.get_site_config_file()[constants.FileConfig.SITE.value][site][constants.FileConfig.DISPLAY_NAME.value],
        "cdm_source_abbreviation": site,
        "cdm_holder": "NIH/NCI Connect for Cancer Prevention Study",
        "source_description": f"Electronic Health Record (EHR) data from {site}",
        "source_documentation_reference": "",
        "cdm_etl_reference": "",
        "source_release_date": delivery_date,
        "cdm_release_date": datetime.today().strftime('%Y-%m-%d'),
        "cdm_version": utils.get_site_config_file()[constants.FileConfig.SITE.value][site][constants.FileConfig.OMOP_VERSION.value],
        "gcs_bucket": utils.get_site_bucket(site),
        "project_id": project_id,
        "dataset_id": dataset_id
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

def create_derived_data_table(site: str, gcs_bucket: str, delivery_date: str, table_name: str, project_id: str, dataset_id: str, vocab_version: str) -> None:
    utils.logger.info(f"Generating derived data table {table_name} for {delivery_date} delivery from {site}")

    utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="populate_derived_data",
        json_data={
            "site": site,
            "gcs_bucket": gcs_bucket,
            "delivery_date": delivery_date,
            "table_name": table_name,
            "project_id": project_id,
            "dataset_id": dataset_id,
            "vocab_version": vocab_version
        }
    )

def populate_cdm_source(cdm_source_data: dict) -> None:
    utils.logger.info(f"If empty, populating cdm_source table for {cdm_source_data['source_release_date']} delivery from {cdm_source_data['cdm_source_abbreviation']}")
    
    utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="populate_cdm_source",
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
