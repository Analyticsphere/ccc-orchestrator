import sys

import requests  # type: ignore
from google.cloud import bigquery  # type: ignore
from google.cloud.exceptions import NotFound  # type: ignore

from . import constants, utils

def load_parquet_to_bq(file_path: str, project_id: str, dataset_id: str) -> None:
    utils.logger.info(f"Loading Parquet file gs://{file_path} to {project_id}.{dataset_id}")
    response = requests.post(
        f"{constants.PROCESSOR_ENDPOINT}/parquet_to_bq",
        headers=utils.get_auth_header(),
        json={
            "file_path": file_path,
            "project_id": project_id,
            "dataset_id": dataset_id
        }
    )
    response.raise_for_status()

def prep_dataset(project_id: str, dataset_id: str) -> None:
    utils.logger.info(f"Clearing dataset {project_id}.{dataset_id}")
    response = requests.post(
        f"{constants.PROCESSOR_ENDPOINT}/clear_bq_dataset",
        headers=utils.get_auth_header(),
        json={
            "project_id": project_id,
            "dataset_id": dataset_id
        }
    )
    response.raise_for_status()

def get_bq_log_row(site, date_to_check) -> list:
    client = bigquery.Client()

    # Check if the table exists. If it doesn't, return an empty list.
    try:
        client.get_table(constants.PIPELINE_LOG_TABLE)
    except NotFound:
        # Table does not exist, so return an empty list without error.
        return []

    # Construct the query to retrieve logs for the given site and delivery date.
    query = f"""
        SELECT *
        FROM `{constants.PIPELINE_LOG_TABLE}`
        WHERE site_name = @site
          AND delivery_date = @delivery_date
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("site", "STRING", site),
            bigquery.ScalarQueryParameter("delivery_date", "DATE", date_to_check),
        ]
    )

    try:
        query_job = client.query(query, job_config=job_config)
        results = list(query_job.result())
        return results
    except Exception as e:
        utils.logger.error(f"Failed to retrieve BigQuery pipeline logs for site '{site}' and date '{date_to_check}': {e}")
        raise Exception(f"Failed to retrieve BigQuery pipeline logs for site '{site}' and date '{date_to_check}': {e}") from e

def bq_log_start(site: str, delivery_date: str, file_type: str, omop_version: str, run_id: str) -> None:
    status = constants.PIPELINE_START_STRING

    response = requests.post(
        f"{constants.PROCESSOR_ENDPOINT}/pipeline_log",
        headers=utils.get_auth_header(),
        json={
            "logging_table": constants.PIPELINE_LOG_TABLE,
            "site_name": site,
            "delivery_date": delivery_date,
            "status": status,
            "file_type": file_type,
            "omop_version": omop_version,
            "run_id": run_id
        }
    )
    response.raise_for_status()

def bq_log_running(site: str, delivery_date: str) -> None:
    status = constants.PIPELINE_RUNNING_STRING

    response = requests.post(
        f"{constants.PROCESSOR_ENDPOINT}/pipeline_log",
        headers=utils.get_auth_header(),
        json={
            "logging_table": constants.PIPELINE_LOG_TABLE,
            "site_name": site,
            "delivery_date": delivery_date,
            "status": status
        }
    )
    response.raise_for_status()

def bq_log_error(run_id: str, message: str) -> None:
    status = constants.PIPELINE_ERROR_STRING

    response = requests.post(
        f"{constants.PROCESSOR_ENDPOINT}/pipeline_log",
        headers=utils.get_auth_header(),
        json={
            "logging_table": constants.PIPELINE_LOG_TABLE,
            "status": status,
            "run_id": run_id,
            "message": message
        }
    )
    response.raise_for_status()

def bq_log_complete(site: str, delivery_date: str) -> None:
    status = constants.PIPELINE_COMPLETE_STRING

    response = requests.post(
        f"{constants.PROCESSOR_ENDPOINT}/pipeline_log",
        headers=utils.get_auth_header(),
        json={
            "logging_table": constants.PIPELINE_LOG_TABLE,
            "site_name": site,
            "delivery_date": delivery_date,
            "status": status
        }
    )
    response.raise_for_status()