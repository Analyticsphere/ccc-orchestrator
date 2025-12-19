from dependencies.ehr import constants, utils


def load_individual_parquet_to_bq(
    file_path: str,
    project_id: str,
    dataset_id: str,
    table_name: str,
    write_type: constants.BQWriteTypes,
    site: str = None,
    delivery_date: str = None
) -> None:
    """Load a single Parquet file to BigQuery."""
    log_ctx = utils.format_log_context(site=site, delivery_date=delivery_date, file=table_name)
    utils.logger.info(f"{log_ctx}Loading Parquet file to BigQuery: {project_id}.{dataset_id}.{table_name}")

    utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="parquet_to_bq",
        json_data={
            "file_path": file_path,
            "project_id": project_id,
            "dataset_id": dataset_id,
            "table_name": table_name,
            "write_type": write_type
        },
        site=site,
        delivery_date=delivery_date,
        file=table_name
    )

def load_harmonized_tables_to_bq(gcs_bucket: str, delivery_date: str, project_id: str, dataset_id: str, site: str = None) -> None:
    """Load all harmonized OMOP-to-OMOP ETL Parquet files to BigQuery."""
    
    log_ctx = utils.format_log_context(site=site, delivery_date=delivery_date)
    utils.logger.info(f"{log_ctx}Loading harmonized/OMOP-to-OMOP ETL Parquet files to BigQuery: {project_id}.{dataset_id}")

    utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="harmonized_parquets_to_bq",
        json_data={
            "bucket": gcs_bucket,
            "delivery_date": delivery_date,
            "project_id": project_id,
            "dataset_id": dataset_id
        },
        site=site,
        delivery_date=delivery_date
    )

def prep_dataset(project_id: str, dataset_id: str, site: str = None, delivery_date: str = None) -> None:
    """Clear/prepare a BigQuery dataset for loading."""
    
    log_ctx = utils.format_log_context(site=site, delivery_date=delivery_date)
    utils.logger.info(f"{log_ctx}Clearing/preparing BigQuery dataset: {project_id}.{dataset_id}")

    utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="clear_bq_dataset",
        json_data={
            "project_id": project_id,
            "dataset_id": dataset_id
        },
        site=site,
        delivery_date=delivery_date
    )

def get_bq_log_row(site: str, delivery_date: str) -> list:
    """Retrieve pipeline logging data from BigQuery.

    Args:
        site: Site identifier
        delivery_date: Delivery date (YYYY-MM-DD format)

    Returns:
        List containing log row data
    """
    
    log_ctx = utils.format_log_context(site=site, delivery_date=delivery_date)
    utils.logger.info(f"{log_ctx}Retrieving pipeline logging data from BigQuery")

    try:
        response = utils.make_api_call(
            url=constants.OMOP_PROCESSOR_ENDPOINT,
            endpoint="get_log_row",
            method="get",
            params={
                "site": site,
                "delivery_date": delivery_date,
            },
            site=site,
            delivery_date=delivery_date
        )

        if response and 'log_row' in response:
            return response['log_row']
        return []

    except Exception as e:
        raise Exception(f"{log_ctx}Error retrieving logging data from BigQuery: {e}") from e

def bq_log_start(site: str, delivery_date: str, file_type: str, omop_version: str, run_id: str) -> None:
    status = constants.PIPELINE_START_STRING

    utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="pipeline_log",
        json_data={
            "site_name": site,
            "delivery_date": delivery_date,
            "status": status,
            "file_type": file_type,
            "omop_version": omop_version,
            "run_id": run_id
        }
    )

def bq_log_running(site: str, delivery_date: str, run_id: str) -> None:
    status = constants.PIPELINE_RUNNING_STRING

    utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="pipeline_log",
        json_data={
            "site_name": site,
            "delivery_date": delivery_date,
            "status": status,
            "run_id": run_id
        }
    )

def bq_log_error(site: str, delivery_date: str, run_id: str, message: str) -> None:
    status = constants.PIPELINE_ERROR_STRING

    utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="pipeline_log",
        json_data={
            "site_name": site,
            "delivery_date": delivery_date,
            "status": status,
            "run_id": run_id,
            "message": message
        }
    )

def bq_log_complete(site: str, delivery_date: str, run_id: str) -> None:
    status = constants.PIPELINE_COMPLETE_STRING

    utils.make_api_call(
        url=constants.OMOP_PROCESSOR_ENDPOINT,
        endpoint="pipeline_log",
        json_data={
            "site_name": site,
            "delivery_date": delivery_date,
            "status": status,
            "run_id": run_id
        }
    )
