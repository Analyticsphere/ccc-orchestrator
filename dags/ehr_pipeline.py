import airflow
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task, task_group
from datetime import datetime, timedelta
import dependencies.utils as utils
import dependencies.constants as constants
import dependencies.processing as processing
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

        utils.logger.info(f"The API is healthy! Reponse: {result}")
    except Exception as e:
        utils.logger.error(f"API health check failed: {str(e)}")
        sys.exit(1)


@task(task_id='get_file_list')
def get_files() -> list[str]:
    utils.logger.info("Executing get_files() task")
    site = 'synthea'

    try:
        result = processing.get_file_list(site)
        utils.logger.info(f"Files for {site} are: {result}")
    except Exception as e:
        utils.logger.error(f"Unable to get file list: {str(e)}")
        sys.exit(1)
    print()

@task(task_id='print_each_file_name')
def print_file_names(var: list[str]) -> None:
    print()

with dag:
    api_health_check = check_api_health()
    file_list = get_files()

api_health_check >> file_list