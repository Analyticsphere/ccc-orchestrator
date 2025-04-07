from dependencies.ehr import utils
from dependencies.ehr import constants


def load_vocabulary_table_gcs_to_bq(vocab_version: str, vocab_gcs_bucket: str, table_file_name: str, project_id: str, dataset_id: str) -> None:
    utils.logger.info(f"Loading {table_file_name} vocabulary table to {project_id}.{dataset_id}")
    utils.make_api_call(
        url=constants.PROCESSOR_URL,
        endpoint="load_target_vocab",
        json_data={
            "vocab_version": vocab_version,
            "vocab_gcs_bucket": vocab_gcs_bucket,
            "table_file_name": table_file_name,
            "project_id": project_id,
            "dataset_id": dataset_id,
        }
    )


def create_optimized_vocab(vocab_version: str, vocab_gcs_bucket: str) -> None:
    utils.logger.info(f"Creating optimized version of {vocab_version} if required")

    utils.make_api_call(
        url=constants.PROCESSOR_URL,
        endpoint="create_optimized_vocab",
        json_data={
            "vocab_version": vocab_version,
            "vocab_gcs_bucket": vocab_gcs_bucket
        }
    )

def harmonize(vocab_version: str, vocab_gcs_bucket: str, omop_version: str, file_path: str, site: str, project_id: str, dataset_id: str) -> None:
    utils.logger.info(f"Standardizing {file_path} to vocabulary version {vocab_version}")

    utils.make_api_call(
        url=constants.PROCESSOR_URL,
        endpoint="harmonize_vocab",
        json_data={
            "vocab_version": vocab_version,
            "vocab_gcs_bucket": vocab_gcs_bucket,
            "omop_version": omop_version,
            "file_path": file_path,
            "site": site,
            "project_id": project_id,
            "dataset_id": dataset_id
        }
    )
