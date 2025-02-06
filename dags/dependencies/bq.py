from . import utils
from . import constants
import requests
from google.cloud import storage
from datetime import datetime

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