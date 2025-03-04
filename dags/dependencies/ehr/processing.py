from dependencies.ehr import utils


def get_file_list(site: str, delivery_date: str, file_format: str) -> list[str]:
    """
    Get a list of files from a site's latest delivery
    """
    try:
        gcs_bucket = utils.get_site_bucket(site)
        full_path = f"{gcs_bucket}/{delivery_date}"
        create_artifact_buckets(full_path)

        utils.logger.info(f"Getting files for {delivery_date} delivery from {site}")

        response = utils.make_api_call(
            endpoint="get_file_list",
            method="get",
            params={
                "bucket": gcs_bucket,
                "folder": delivery_date,
                "file_format": file_format
            }
        )

        if response and 'file_list' in response:
            return response['file_list']
        return []

    except Exception as e:
        utils.logger.error(f"Error getting file list: {e}")
        raise Exception(f"Error getting file list: {e}") from e

def create_artifact_buckets(parent_bucket: str) -> None:
    """
    Create artifact buckets in the parent bucket for storing processing artifacts
    """
    utils.logger.info(f"Creating artifact bucket in {parent_bucket}")
    
    utils.make_api_call(
        endpoint="create_artifact_buckets",
        json_data={"parent_bucket": parent_bucket}
    )

def process_file(file_type: str, gcs_file_path: str) -> None:
    """
    Create optimized version of incoming EHR data file.
    """
    utils.logger.info(f"Processing incoming {file_type} file gs://{gcs_file_path}")
    
    utils.make_api_call(
        endpoint="process_incoming_file",
        json_data={
            "file_type": file_type,
            "file_path": gcs_file_path
        },
        timeout=(10, 600)  # Long timeout for large file conversions
    )

def normalize_parquet_file(file_path: str, cdm_version: str) -> None:
    """
    Standardize OMOP data file structure.
    """
    utils.logger.info(f"Normalizing Parquet file gs://{file_path}")
    
    utils.make_api_call(
        endpoint="normalize_parquet",
        json_data={
            "file_path": file_path,
            "omop_version": cdm_version
        }
    )

def upgrade_cdm(file_path: str, cdm_version: str, target_cdm_version: str) -> None:
    """
    Upgrade CDM version of the file (e.g., from 5.3 to 5.4)
    """
    utils.logger.info(f"Upgrading CDM version {cdm_version} of file gs://{file_path} to {target_cdm_version}")
    
    utils.make_api_call(
        endpoint="upgrade_cdm",
        json_data={
            "file_path": file_path,
            "omop_version": cdm_version,
            "target_omop_version": target_cdm_version
        }
    )