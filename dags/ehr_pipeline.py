import airflow
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task, task_group
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
from dependencies import utils
import logging
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
def check_api_health():
    """
    Task to verify EHR processing container is up
    """
    logging.info("Executing check_api_health() task")
    try:
        result = utils.check_service_health("https://ccc-ehr-pipeline-1061430463455.us-central1.run.app")
        if result['status'] != 'healthy':
            logging.error(f"API health check failed. Status: {result['status']}")
            sys.exit(1)

        logging.info(f"The API is healthy! Reponse: \n{result}")
        #return True
    except Exception as e:
        logging.error(f"API health check failed: {str(e)}")
        sys.exit(1)


with dag:
    api_health_check = check_api_health()
