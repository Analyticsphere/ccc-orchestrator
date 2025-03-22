from enum import Enum

# Main endpoint
PROCESSOR_URL = "https://ccc-omop-file-processor-eaf-1061430463455.us-central1.run.app"
ANALAYZER_URL = "https://ccc-omop-analyzer-1061430463455.us-central1.run.app"

SITE_CONFIG_YML_PATH = "/home/airflow/gcs/dags/dependencies/ehr/config/site_config.yml"

PIPELINE_LOG_TABLE = "nih-nci-dceg-connect-dev.ehr_pipeline_metadata.pipeline_runs"
PIPELINE_START_STRING = "started"
PIPELINE_RUNNING_STRING = "running"
PIPELINE_COMPLETE_STRING = "completed"
PIPELINE_ERROR_STRING = "error"
PIPELINE_DAG_FAIL_MESSAGE = "DAG failed"

# When True, overwrites site provided vocabulary tables with target vocabulary tables from Athena
LOAD_ONLY_TARGET_VOCAB = True
TARGET_VOCAB_VERSION = "v5.0 30-AUG-24"
VOCAB_REF_GCS_BUCKET = "ehr_pipeline_vocabulary_files"
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

TARGET_CDM_VERSION = "5.4"

CONDITION_ERA = "condition_era"
DRUG_ERA = "drug_era"
OBSERVATION_PERIOD = "observation_period"
DERIVED_DATA_TABLES: list = [DRUG_ERA, CONDITION_ERA, OBSERVATION_PERIOD]

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
    CREATED_FILES = f"{ARTIFACTS}created_files/"
    REPORT = f"{ARTIFACTS}delivery_report/"
    REPORT_TMP = f"{ARTIFACTS}delivery_report/tmp/"
    DQD = f"{ARTIFACTS}dqd/"
    ACHILLES = f"{ARTIFACTS}achilles/"
    INVALID_ROWS = f"{ARTIFACTS}invalid_rows/"