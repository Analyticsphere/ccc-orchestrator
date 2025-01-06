from enum import Enum

PROCESSOR_ENDPOINT = "https://ccc-omop-file-processor-1061430463455.us-central1.run.app"
SITE_CONFIG_YML_PATH = "/home/airflow/gcs/dags/config/site_config.yml"

class FileConfig(str, Enum):
    DISPLAY_NAME = "display_name"
    GCS_PATH = "gcs_path"
    FILE_DELIVERY_FORMAT = "file_delivery_format"
    PROJECT_ID = "project_id"
    BQ_DATASET = "bq_dataset"
    OMOP_VERSION = "omop_version"
    SITE = "site"
    DELIVERY_DATE = "delivery_date"
    FILE_NAME = "file_name"