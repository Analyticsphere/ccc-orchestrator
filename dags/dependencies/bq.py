from . import utils
from . import constants
import requests
from google.cloud import storage, bigquery
from datetime import datetime
import sys

def load_parquet_to_bq(file_path: str, project_id: str, dataset_id: str) -> None:
    utils.logger.info(f"Loading Parquet file gs://{file_path} to {project_id}.{dataset_id}")
    response = requests.get(
        f"{constants.PROCESSOR_ENDPOINT}/parquet_to_bq?file_path={file_path}&project_id={project_id}&dataset_id={dataset_id}",
        headers=utils.get_auth_header()
    )
    response.raise_for_status()

def prep_dataset(project_id: str, dataset_id: str) -> None:
    utils.logger.info(f"Clearing dataset {project_id}.{dataset_id}")
    response = requests.get(
        f"{constants.PROCESSOR_ENDPOINT}/clear_bq_dataset?project_id={project_id}&dataset_id={dataset_id}",
        headers=utils.get_auth_header()
    )
    response.raise_for_status()

def get_bq_log_row(site, date_to_check) -> list:
    # Construct a query to check if the site and delivery date exist in the table,
    # and if so, what the status is.
    try:
        client = bigquery.Client()

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

        query_job = client.query(query, job_config=job_config)
        results = list(query_job.result())

        return results
    except Exception as e:
        utils.logger.error(f"Unable to retrive pipeline logs: {e}")
        sys.exit(1)