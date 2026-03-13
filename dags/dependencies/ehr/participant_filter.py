from dependencies.ehr import constants, utils
from dependencies.ehr.utils import format_log_context


def get_connect_data(
    project_id: str,
    dataset_id: str = constants.CONNECT_DATASET_ID,
    delivery_bucket: str = None,
    site_connect_id: int = None,
    site: str = None,
    delivery_date: str = None,
) -> None:
    """
    Export Connect study data for a site delivery.

    Args:
        project_id: BigQuery project ID
        dataset_id: BigQuery dataset ID
        delivery_bucket: Full GCS delivery bucket path (bucket/delivery_date)
        site_connect_id: Per-site Connect identifier
        site: Optional site identifier for logging context
        delivery_date: Optional delivery date for logging context
    """
    log_ctx = format_log_context(site=site, delivery_date=delivery_date)
    utils.logger.info(f"{log_ctx}Exporting Connect study data to Parquet")

    utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="get_connect_data",
        json_data={
            "project_id": project_id,
            "dataset_id": dataset_id,
            "delivery_bucket": delivery_bucket,
            "site_connect_id": site_connect_id
        },
        log_site=site,
    )


def filter_connect_participants(
    file_path: str,
    site: str = None,
    delivery_date: str = None,
    file: str = None
) -> None:
    """
    Apply Connect participant-status exclusions to a single file.

    This endpoint is intended to run after normalization/CDM upgrade and before
    vocabulary harmonization. The processor service will return a successful
    response for tables that do not contain a person_id column, so the DAG can
    keep one simple file-level task for all tables.

    Args:
        file_path: GCS path used by the processor to locate the file artifact
        site: Optional site identifier for logging context
        delivery_date: Optional delivery date for logging context
        file: Optional table/file name for logging context
    """
    log_ctx = format_log_context(site=site, delivery_date=delivery_date, file=file)
    utils.logger.info(f"{log_ctx}Applying Connect participant filtering")

    utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="filter_connect_participants",
        json_data={
            "file_path": file_path,
        },
        log_site=site,
        log_delivery_date=delivery_date,
        log_file=file
    )
