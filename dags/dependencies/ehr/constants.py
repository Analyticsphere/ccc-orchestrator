from enum import Enum

# Main endpoint
PROCESSOR_ENDPOINT = "https://ccc-omop-file-processor-155089172944.us-central1.run.app"

SITE_CONFIG_YML_PATH = "/home/airflow/gcs/dags/dependencies/ehr/config/site_config.yml"

PIPELINE_START_STRING = "started"
PIPELINE_RUNNING_STRING = "running"
PIPELINE_COMPLETE_STRING = "completed"
PIPELINE_ERROR_STRING = "error"
PIPELINE_DAG_FAIL_MESSAGE = "DAG failed"

DEFAULT_CONNECTION_TIMEOUT = 60
DEFAULT_READ_TIMEOUT = 1800

# When True, overwrites site provided vocabulary tables with target vocabulary tables from Athena
LOAD_ONLY_TARGET_VOCAB = True
TARGET_VOCAB_VERSION = "v5.0 30-AUG-24"
VOCABULARY_TABLES = [
    "concept",
    "concept_ancestor",
    "concept_class",
    "concept_relationship",
    "concept_synonym",
    "domain",
    "drug_strength",
    "relationship",
    "vocabulary"
]

VOCAB_HARMONIZED_TABLES = [
    "visit_occurrence",
    "condition_occurrence",
    "drug_exposure",
    "procedure_occurrence",
    "device_exposure",
    "measurement",
    "observation",
    "note",
    "specimen"
]

TARGET_CDM_VERSION = "5.4"

CONDITION_ERA = "condition_era"
DRUG_ERA = "drug_era"
OBSERVATION_PERIOD = "observation_period"
DERIVED_DATA_TABLES: list = [DRUG_ERA, CONDITION_ERA, OBSERVATION_PERIOD]

CSV = ".csv"
PARQUET = ".parquet"

class FileConfig(str, Enum):
    DISPLAY_NAME = "display_name"
    GCS_BUCKET = "gcs_bucket"
    FILE_DELIVERY_FORMAT = "file_delivery_format"
    PROJECT_ID = "project_id"
    BQ_DATASET = "bq_dataset"
    OMOP_VERSION = "omop_version"
    SITE = "site"
    DELIVERY_DATE = "delivery_date"
    SOURCE_FILE = "source_file"
    FILE_PATH = "file_path"
    TABLE_NAME = "table_name"

class ArtifactPaths(str, Enum):
    ARTIFACTS = "artifacts/"
    FIXED_FILES = f"{ARTIFACTS}fixed_files/"
    CONVERTED_FILES = f"{ARTIFACTS}converted_files/"
    CREATED_FILES = f"{ARTIFACTS}created_files/"
    REPORT = f"{ARTIFACTS}delivery_report/"
    REPORT_TMP = f"{ARTIFACTS}delivery_report/tmp/"
    DQD = f"{ARTIFACTS}dqd/"
    ACHILLES = f"{ARTIFACTS}achilles/"
    INVALID_ROWS = f"{ARTIFACTS}invalid_rows/"

class BQWriteTypes(str, Enum):
    SPECIFIC_FILE = "specific_file"
    ETLed_FILE = "ETLed_file"
    PROCESSED_FILE = "processed_file"