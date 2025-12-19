from dependencies.ehr import constants, utils
from dependencies.ehr.storage_backend import storage
from dependencies.ehr.utils import format_log_context


def get_file_list(site: str, delivery_date: str, file_format: str) -> list[str]:
    """
    Get a list of files from a site's latest delivery.

    Args:
        site: Site identifier
        delivery_date: Delivery date (YYYY-MM-DD format)
        file_format: File format filter (e.g., '.csv', '.parquet')

    Returns:
        List of file paths found in the delivery bucket
    """
    

    try:
        gcs_bucket = utils.get_site_bucket(site=site)
        full_path = f"{gcs_bucket}/{delivery_date}"
        create_artifact_directories(delivery_bucket=full_path, site=site, delivery_date=delivery_date)

        log_ctx = format_log_context(site=site, delivery_date=delivery_date)
        utils.logger.info(f"{log_ctx}Discovering {file_format} files in delivery bucket: {full_path}")

        response = utils.make_api_call(
            url=constants.OMOP_PROCESSOR_ENDPOINT,
            endpoint="get_file_list",
            method="get",
            params={
                "bucket": gcs_bucket,
                "folder": delivery_date,
                "file_format": file_format
            },
            site=site,
            delivery_date=delivery_date
        )

        if response and 'file_list' in response:
            file_list = response['file_list']
            utils.logger.info(f"{log_ctx}Discovered {len(file_list)} file(s) to process")
            return file_list
        utils.logger.info(f"{log_ctx}No files found matching format: {file_format}")
        return []

    except Exception as e:
        log_ctx = format_log_context(site=site, delivery_date=delivery_date)
        raise Exception(f"{log_ctx}Error getting file list: {e}") from e

def create_artifact_directories(delivery_bucket: str, site: str = None, delivery_date: str = None) -> None:
    """
    Create artifact directories in the parent bucket for storing processing artifacts.

    Args:
        delivery_bucket: Full GCS path to delivery bucket (bucket/delivery_date)
        site: Optional site identifier for logging context
        delivery_date: Optional delivery date for logging context
    """
    
    log_ctx = format_log_context(site=site, delivery_date=delivery_date)
    utils.logger.info(f"{log_ctx}Creating artifact directories in: {delivery_bucket}")

    utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="create_artifact_directories",
        json_data={"delivery_bucket": delivery_bucket},
        site=site,
        delivery_date=delivery_date
    )

def process_file(file_type: str, gcs_file_path: str, site: str = None, delivery_date: str = None) -> None:
    """
    Create optimized version of incoming EHR data file.

    Args:
        file_type: OMOP table name
        gcs_file_path: Full GCS path to the file
        site: Optional site identifier for logging context
        delivery_date: Optional delivery date for logging context
    """
    
    log_ctx = format_log_context(site=site, delivery_date=delivery_date, file=file_type)
    utils.logger.info(f"{log_ctx}Processing incoming file: {storage.get_uri(gcs_file_path)}")

    utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="process_incoming_file",
        json_data={
            "file_type": file_type,
            "file_path": gcs_file_path
        },
        site=site,
        delivery_date=delivery_date,
        file=file_type
    )

def normalize_parquet_file(file_path: str, cdm_version: str, date_format: str, datetime_format: str, site: str = None, delivery_date: str = None, file: str = None) -> None:
    """
    Standardize OMOP data file structure.

    Args:
        file_path: GCS path to Parquet file
        cdm_version: OMOP CDM version
        date_format: Date format string
        datetime_format: Datetime format string
        site: Optional site identifier for logging context
        delivery_date: Optional delivery date for logging context
        file: Optional file/table name for logging context
    """
    
    log_ctx = format_log_context(site=site, delivery_date=delivery_date, file=file)
    utils.logger.info(f"{log_ctx}Normalizing Parquet file (CDM {cdm_version})")

    utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="normalize_parquet",
        json_data={
            "file_path": file_path,
            "omop_version": cdm_version,
            "date_format": date_format,
            "datetime_format": datetime_format
        },
        site=site,
        delivery_date=delivery_date,
        file=file
    )

