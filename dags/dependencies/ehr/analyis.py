'''Run HADES R packages to analyze the OMOP data, e.g., DataQualityDashboard and Achilles.'''
from dependencies.ehr import utils
from dependencies.ehr import constants

def run_dqd(project_id: str, dataset_id: str) -> None:
    utils.logger.info(f"Running dqd_checks for {project_id}.{dataset_id}")
    utils.make_api_call(
        url = constants.ANALAYZER_URL,
        endpoint="run_dqd",
        json_data={
            "project_id": project_id,
            "dataset_id": dataset_id,            
        }
    )

def run_achilles(project_id: str, dataset_id: str) -> None:
    utils.logger.info(f"Running Achilles for {project_id}.{dataset_id}")
    utils.make_api_call(
        url = constants.ANALAYZER_URL,
        endpoint="run_achilles",
        json_data={
            "project_id": project_id,
            "dataset_id": dataset_id,            
        }
    )