from enum import Enum

# Main endpoint
#PROCESSOR_ENDPOINT = "https://ccc-omop-file-processor-1061430463455.us-central1.run.app"
PROCESSOR_ENDPOINT = "https://ccc-omop-file-processor-jp-1061430463455.us-central1.run.app"

SITE_CONFIG_YML_PATH = "/home/airflow/gcs/dags/config/site_config.yml"

PIPELINE_LOG_TABLE = "nih-nci-dceg-connect-dev.ehr_pipeline_metadata.pipeline_runs"
PIPELINE_START_STRING = "started"
PIPELINE_RUNNING_STRING = "running"
PIPELINE_COMPLETE_STRING = "completed"
PIPELINE_ERROR_STRING = "error"
PIPELINE_DAG_FAIL_MESSAGE = "DAG failed"


CSV = ".csv"
PARQUET = ".parquet"

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

class ArtifactPaths(str, Enum):
    ARTIFACTS = "artifacts/"
    FIXED_FILES = f"{ARTIFACTS}fixed_files/"
    CONVERTED_FILES = f"{ARTIFACTS}converted_files/"
    REPORT = f"{ARTIFACTS}delivery_report/"
    REPORT_TMP = f"{ARTIFACTS}delivery_report/tmp/"
    DQD = f"{ARTIFACTS}dqd/"
    INVALID_ROWS = f"{ARTIFACTS}invalid_rows/"