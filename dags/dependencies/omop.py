from datetime import datetime

from . import constants, utils

def generate_report_json(site: str, delivery_date: str) -> dict:
    # Generate final data delivery report
    report_data = {
        "site": site,
        "gcs_bucket": utils.get_site_bucket(site),
        "delivery_date": delivery_date,
        "site_display_name": utils.get_site_config_file()[constants.FileConfig.SITE.value][site][constants.FileConfig.DISPLAY_NAME.value],
        "file_delivery_format": utils.get_site_config_file()[constants.FileConfig.SITE.value][site][constants.FileConfig.FILE_DELIVERY_FORMAT.value],
        "delivered_cdm_version": utils.get_site_config_file()[constants.FileConfig.SITE.value][site][constants.FileConfig.OMOP_VERSION.value],
        "target_vocabulary_version": constants.TARGET_VOCAB_VERSION,
        "target_cdm_version": constants.TARGET_CDM_VERSION,
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

def create_optimized_vocab(vocab_version: str, vocab_gcs_bucket: str) -> None:
    utils.logger.info(f"Creating optimized version of {vocab_version} if required")
    
    utils.make_api_call(
        endpoint="create_optimized_vocab",
        json_data={
            "vocab_version": vocab_version,
            "vocab_gcs_bucket": vocab_gcs_bucket
        }
    )

def create_missing_omop_tables(project_id: str, dataset_id: str, omop_version: str) -> None:
    utils.logger.info(f"Creating any missing OMOP tables in {project_id}.{dataset_id}")
    
    utils.make_api_call(
        endpoint="create_missing_tables",
        json_data={
            "omop_version": omop_version,
            "project_id": project_id,
            "dataset_id": dataset_id
        }
    )

def populate_cdm_source(cdm_source_data: dict) -> None:
    utils.logger.info(f"If empty, populating cdm_source table for {cdm_source_data['source_release_date']} delivery from {cdm_source_data['cdm_source_abbreviation']}")
    
    utils.make_api_call(
        endpoint="populate_cdm_source",
        json_data=cdm_source_data
    )