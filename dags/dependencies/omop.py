import requests  # type: ignore

from . import constants, utils

def create_optimized_vocab(vocab_version: str, vocab_gcs_bucket: str) -> None:
    utils.logger.info(f"Creating optimized version of {vocab_version} if required")
    response = requests.get(
        f"{constants.PROCESSOR_ENDPOINT}/create_optimized_vocab?vocab_version={vocab_version}&vocab_gcs_bucket={vocab_gcs_bucket}",
        headers=utils.get_auth_header()
    )
    response.raise_for_status()

def create_missing_omop_tables(project_id: str, dataset_id: str, omop_version: str) -> None:
    utils.logger.info(f"Creating any missing OMOP tables in {project_id}.{dataset_id}")
    response = requests.get(
        f"{constants.PROCESSOR_ENDPOINT}/create_missing_tables?omop_version={omop_version}&project_id={project_id}&dataset_id={dataset_id}",
        headers=utils.get_auth_header()
    )
    response.raise_for_status()