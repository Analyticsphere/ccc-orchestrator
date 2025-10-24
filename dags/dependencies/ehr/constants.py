from enum import Enum
import os

# Environmental variables from Airflow/Cloud Composer
OMOP_PROCESSOR_ENDPOINT = os.getenv('OMOP_PROCESSOR_ENDPOINT', 'NO OMOP_PROCESSOR_ENDPOINT DEFINED')
OMOP_ANALYZER_ENDPOINT = os.getenv('OMOP_ANALYZER_ENDPOINT', 'NO OMOP_ANALYZER_ENDPOINT DEFINED')
OMOP_TARGET_VOCAB_VERSION = os.getenv('OMOP_TARGET_VOCAB_VERSION', 'v5.0 27-AUG-25')
OMOP_TARGET_CDM_VERSION = os.getenv('OMOP_TARGET_CDM_VERSION', '5.4')
OMOP_VOCAB_GCS_PATH = os.getenv('OMOP_VOCAB_GCS_PATH', 'NO OMOP_VOCAB_GCS_PATH DEFINED')

SITE_CONFIG_YML_PATH = "/home/airflow/gcs/dags/dependencies/ehr/config/site_config.yml"

PIPELINE_START_STRING = "started"
PIPELINE_RUNNING_STRING = "running"
PIPELINE_COMPLETE_STRING = "completed"
PIPELINE_ERROR_STRING = "error"
PIPELINE_DAG_FAIL_MESSAGE = "DAG failed"

DEFAULT_CONNECTION_TIMEOUT_SEC = 60
VOCAB_TIME_MIN = 60
VOCAB_TIMEOUT_SEC = VOCAB_TIME_MIN * 60
VOCAB_HARMONIZATION_RETRYS = 60

# Vocabulary harmonization step constants
SOURCE_TARGET = "Map source concepts to updated target codes"
TARGET_REMAP = "Remap non-standard targets to new standard targets"
TARGET_REPLACEMENT = "Replace non-standard targets with new standard targets"
DOMAIN_CHECK = "Check for latest domain and update if needed"
OMOP_ETL = "OMOP to OMOP ETL"

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

CONDITION_ERA = "condition_era"
DRUG_ERA = "drug_era"
OBSERVATION_PERIOD = "observation_period"
DERIVED_DATA_TABLES: list = [DRUG_ERA, CONDITION_ERA, OBSERVATION_PERIOD]

CSV = ".csv"
CSV_GZ = ".csv.gz"
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
    DATE_FORMAT = "date_format"
    DATETIME_FORMAT = "datetime_format"
    OVERWRITE_SITE_VOCAB_WITH_STANDARD = "overwrite_site_vocab_with_standard"

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