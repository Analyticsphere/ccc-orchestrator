import airflow
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task, task_group
from datetime import datetime, timedelta
import dependencies.utils as utils
import dependencies.constants as constants
import dependencies.processing as processing
import dependencies.validation as validation
import dependencies.file_config as file_config
import sys


default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'owner': 'airflow'
}

dag = DAG(
    'ehr-pipeline',
    default_args=default_args,
    description='Pipeline for processing EHR OMOP data files',
    start_date=days_ago(1)
)

@task(task_id='check_api_health')
def check_api_health() -> None:
    """
    Task to verify EHR processing container is up
    """
    utils.logger.info("Checking OMOP file processor API status")
    try:
        result = utils.check_service_health(constants.PROCESSOR_ENDPOINT)
        if result['status'] != 'healthy':
            utils.logger.error(f"API health check failed. Status: {result['status']}")
            sys.exit(1)

        utils.logger.info(f"The API is healthy! Response: {result}")
    except Exception as e:
        utils.logger.error(f"API health check failed: {str(e)}")
        sys.exit(1)

@task(task_id='get_file_list')
def get_files() -> list[dict]:
    utils.logger.info("Getting list of files to process")
    
    try:
        config = utils.get_site_config_file()
        # Create list of all sites, using the site_config.yml file
        sites = list(config[constants.FileConfig.SITE.value].keys())

        file_configs: list[dict] = []

        # Iterate over each site
        for site in sites:
            # Get a list of files from an individual site
            # get_file_list() also creates artifact buckets for the pipeline for a given site
            files = processing.get_file_list(site)

            for file in files:
                # Create file configuration dictionaries for each file from a site
                file_config_obj = file_config.FileConfig(site, file)
                file_configs.append(file_config_obj.to_dict())
        
        return file_configs
    
    except Exception as e:
        utils.logger.error(f"Unable to get file list: {str(e)}")
        sys.exit(1)

@task(max_active_tis_per_dag=10, execution_timeout=timedelta(minutes=60))
def process_incoming_file(file_config: dict) -> None:
    file_type = f"{file_config[constants.FileConfig.FILE_DELIVERY_FORMAT.value]}"
    gcs_file_path = f"{file_config[constants.FileConfig.GCS_PATH.value]}/{file_config[constants.FileConfig.DELIVERY_DATE.value]}/{file_config[constants.FileConfig.FILE_NAME.value]}"

    processing.process_file(file_type, gcs_file_path)

@task(max_active_tis_per_dag=10, execution_timeout=timedelta(minutes=60))
def validate_file(file_config: dict) -> None:
    validation.validate_file(
        file_path=f"gs://{file_config[constants.FileConfig.GCS_PATH.value]}/{file_config[constants.FileConfig.DELIVERY_DATE.value]}/{file_config[constants.FileConfig.FILE_NAME.value]}", 
        omop_version=file_config[constants.FileConfig.OMOP_VERSION.value],
        gcs_path=file_config[constants.FileConfig.GCS_PATH.value],
        delivery_date=file_config[constants.FileConfig.DELIVERY_DATE.value]
        )

@task(max_active_tis_per_dag=10, execution_timeout=timedelta(minutes=60))
def fix_parquet(file_config: dict) -> None:
    file_name = file_config[constants.FileConfig.FILE_NAME.value].replace(file_config[constants.FileConfig.FILE_DELIVERY_FORMAT.value], '')
    file_path = f"{file_config[constants.FileConfig.GCS_PATH.value]}/{file_config[constants.FileConfig.DELIVERY_DATE.value]}/{constants.ArtifactPaths.CONVERTED_FILES.value}{file_name}{constants.PARQUET}"

    omop_version = file_config[constants.FileConfig.OMOP_VERSION.value]

    utils.logger.info(f"Fixing Parquet file gs://{file_path}")
    processing.fix_parquet_file(file_path, omop_version)

# @task(max_active_tis_per_dag=10)
# def dummy_testing_task(file_config: dict) -> None:
#     utils.logger.info(f"Going to validate schema of gs://{file_config[constants.FileConfig.GCS_PATH.value]}/{file_config[constants.FileConfig.DELIVERY_DATE.value]}/{file_config[constants.FileConfig.FILE_NAME.value]} against OMOP v{file_config[constants.FileConfig.OMOP_VERSION.value]}")
#     utils.logger.info(f"Will write to BQ dataset {file_config[constants.FileConfig.PROJECT_ID.value]}.{file_config[constants.FileConfig.BQ_DATASET.value]}")

with dag:
    api_health_check = check_api_health()
    file_list = get_files()
    process_files = process_incoming_file.expand(file_config=file_list)
    validate_files = validate_file.expand(file_config=file_list)
    fix_file = fix_parquet.expand(file_config=file_list)

api_health_check >> file_list >> process_files >> validate_files >> fix_file