from dependencies.ehr import constants, utils
from google.cloud import bigquery  # type: ignore
from google.cloud.exceptions import NotFound  # type: ignore


def load_parquet_to_bq(file_path: str, project_id: str, dataset_id: str, table_name: str, write_type: constants.BQWriteTypes) -> None:
    utils.logger.info(f"Loading Parquet file gs://{file_path} to {project_id}.{dataset_id}")
    
    utils.make_api_call(
        endpoint="parquet_to_bq",
        json_data={
            "file_path": file_path,
            "project_id": project_id,
            "dataset_id": dataset_id,
            "table_name": table_name,
            "write_type": write_type
        }
    )

def prep_dataset(project_id: str, dataset_id: str) -> None:
    utils.logger.info(f"Clearing dataset {project_id}.{dataset_id}")
    
    utils.make_api_call(
        endpoint="clear_bq_dataset",
        json_data={
            "project_id": project_id,
            "dataset_id": dataset_id
        }
    )

def get_bq_log_row(site: str, delivery_date: str) -> list:
    utils.logger.info(f"Getting logging data for {delivery_date} delivery from {site}")

    try:
        response = utils.make_api_call(
            endpoint="get_log_row",
            method="get",
            params={
                "site": site,
                "delivery_date": delivery_date,
            }
        )

        if response and 'log_row' in response:
            return response['log_row']
        return []

    except Exception as e:
        utils.logger.error(f"Error getting logging data: {e}")
        raise Exception(f"Error getting logging data: {e}") from e

def bq_log_start(site: str, delivery_date: str, file_type: str, omop_version: str, run_id: str) -> None:
    status = constants.PIPELINE_START_STRING

    utils.make_api_call(
        endpoint="pipeline_log",
        json_data={
            #"logging_table": constants.PIPELINE_LOG_TABLE,
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
        endpoint="pipeline_log",
        json_data={
            #"logging_table": constants.PIPELINE_LOG_TABLE,
            "site_name": site,
            "delivery_date": delivery_date,
            "status": status,
            "run_id": run_id
        }
    )

def bq_log_error(site: str, delivery_date: str, run_id: str, message: str) -> None:
    status = constants.PIPELINE_ERROR_STRING

    utils.make_api_call(
        endpoint="pipeline_log",
        json_data={
            #"logging_table": constants.PIPELINE_LOG_TABLE,
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
        endpoint="pipeline_log",
        json_data={
            #"logging_table": constants.PIPELINE_LOG_TABLE,
            "site_name": site,
            "delivery_date": delivery_date,
            "status": status,
            "run_id": run_id
        }
    )
