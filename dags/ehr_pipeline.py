import airflow
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task, task_group
from datetime import datetime, timedelta
import dependencies.utils as utils
import dependencies.constants as constants
import dependencies.processing as processing
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
def get_files() -> list[file_config.FileConfig]:
    utils.logger.info("Executing get_files() task")
    
    try:
        config = utils.get_site_config_file()
        sites = list(config['site'].keys())

        file_configs: list[dict] = []

        for site in sites:
            files = processing.get_file_list(site)
            for file in files:
                file_config_obj = file_config.FileConfig(site, file)
                file_configs.append(file_config_obj.to_dict())
        
        return file_configs
    
    except Exception as e:
        utils.logger.error(f"Unable to get file list: {str(e)}")
        sys.exit(1)

@task(max_active_tis_per_dag=10)
def print_file_info(file_config: dict) -> None:
    utils.logger.info(f"The file info is {file_config}")

@task(max_active_tis_per_dag=10)
def dummy_testing_task(file_config: dict) -> None:
    utils.logger.info(f"Going to validate schema of gs://{file_config['gcs_path']}/{file_config['delivery_date']}/{file_config['file_name']} against OMOP v{file_config['omop_version']}")
    utils.logger.info(f"Will write to BQ dataset {file_config['project_id']}.{file_config['bq_table']}")

with dag:
    api_health_check = check_api_health()
    file_list = get_files()
    print_info = print_file_info.expand(file_config=file_list)
    dummy_info = dummy_testing_task.expand(file_config=file_list)

api_health_check >> file_list >> print_info >> dummy_info