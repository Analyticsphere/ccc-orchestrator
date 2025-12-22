from functools import wraps
from typing import Any, Callable, Optional

from airflow.exceptions import AirflowException  # type: ignore
from airflow.operators.python import get_current_context  # type: ignore
from dependencies.ehr import bq, constants, utils


class SiteConfig:
    """
    Provides access to site configuration values.
    """

    def __init__(self, site: str):
        """
        Initialize site configuration.

        Args:
            site: The site name
        """
        self.site = site
        self._config = utils.get_site_config(site=site)

    @property
    def project_id(self) -> str:
        """BigQuery project ID for this site."""
        return self._config[constants.FileConfig.PROJECT_ID.value]

    @property
    def cdm_dataset_id(self) -> str:
        """BigQuery CDM dataset ID for this site."""
        return self._config[constants.FileConfig.CDM_BQ_DATASET.value]

    @property
    def analytics_dataset_id(self) -> str:
        """BigQuery Atlas Results dataset ID for this site."""
        return self._config[constants.FileConfig.ANALYTICS_BQ_DATASET.value]

    @property
    def gcs_bucket(self) -> str:
        """GCS bucket name for this site."""
        return self._config[constants.FileConfig.GCS_BUCKET.value]

    @property
    def display_name(self) -> str:
        """Human-readable display name for this site."""
        return self._config[constants.FileConfig.DISPLAY_NAME.value]

    @property
    def file_delivery_format(self) -> str:
        """File delivery format (e.g., 'csv', 'parquet')."""
        return self._config[constants.FileConfig.FILE_DELIVERY_FORMAT.value]

    @property
    def omop_version(self) -> str:
        """OMOP CDM version for this site."""
        return self._config[constants.FileConfig.OMOP_VERSION.value]

    @property
    def date_format(self) -> Optional[str]:
        """Date format string, if specified."""
        return self._config.get(constants.FileConfig.DATE_FORMAT.value)

    @property
    def datetime_format(self) -> Optional[str]:
        """Datetime format string, if specified."""
        return self._config.get(constants.FileConfig.DATETIME_FORMAT.value)

    @property
    def overwrite_site_vocab_with_standard(self) -> bool:
        """Whether to overwrite site vocabulary with standard vocabulary."""
        return self._config.get(
            constants.FileConfig.OVERWRITE_SITE_VOCAB_WITH_STANDARD.value,
            True
        )


class TaskContext:
    """
    Provides access to Airflow context information.
    """

    @staticmethod
    def get_run_id() -> str:
        """Get the current Airflow DAG run ID."""
        return utils.get_run_id(get_current_context())

    @staticmethod
    def get_context() -> dict:
        """Get the current Airflow task context."""
        return get_current_context()


def log_task_execution(
    site_param: str = 'site',
    delivery_date_param: str = 'delivery_date',
    skip_running_log: bool = False
) -> Callable:
    """
    Decorator that adds automatic logging and error handling to DAG tasks.

    This decorator:
    1. Logs task start (optional, via bq.bq_log_running)
    2. Catches exceptions and logs them via bq.bq_log_error
    3. Re-raises AirflowExceptions without logging (e.g., AirflowSkipException)
    4. Wraps other exceptions with descriptive error messages

    Args:
        site_param: Name of the parameter containing the site value
        delivery_date_param: Name of the parameter containing the delivery_date value
        skip_running_log: If True, skip calling bq.bq_log_running at task start

    Example:
        @task()
        @log_task_execution()
        def my_task(site: str, delivery_date: str) -> None:
            # Task implementation - no try/catch needed!
            pass

        @task()
        @log_task_execution(site_param='site_to_process[0]', delivery_date_param='site_to_process[1]')
        def my_task(site_to_process: tuple[str, str]) -> None:
            # Automatically extracts site and delivery_date from tuple
            pass
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            # Extract site and delivery_date from arguments
            site = None
            delivery_date = None

            import inspect
            sig = inspect.signature(func)
            param_names = list(sig.parameters.keys())

            # Map positional args to parameter names
            arg_dict = dict(zip(param_names, args))
            arg_dict.update(kwargs)

            # Try different extraction strategies
            # 1. Check for file_config_dict parameter
            if 'file_config_dict' in arg_dict:
                file_config_dict = arg_dict['file_config_dict']
                if isinstance(file_config_dict, dict):
                    site = file_config_dict.get(constants.FileConfig.SITE.value)
                    delivery_date = file_config_dict.get(constants.FileConfig.DELIVERY_DATE.value)

            # 2. Check for site_to_process tuple parameter
            elif 'site_to_process' in arg_dict:
                value = arg_dict['site_to_process']
                if isinstance(value, tuple) and len(value) == 2:
                    site, delivery_date = value

            # 3. Check for direct site and delivery_date parameters
            elif site_param in arg_dict and delivery_date_param in arg_dict:
                site = arg_dict[site_param]
                delivery_date = arg_dict[delivery_date_param]

            # Log task running if we have site and delivery_date
            if not skip_running_log and site and delivery_date:
                bq.bq_log_running(site=site, delivery_date=delivery_date, run_id=TaskContext.get_run_id())

            try:
                return func(*args, **kwargs)
            except AirflowException:
                # Re-raise Airflow exceptions (like AirflowSkipException) without logging
                raise
            except Exception as e:
                # Log the error if we have site and delivery_date
                if site and delivery_date:
                    bq.bq_log_error(site=site, delivery_date=delivery_date, run_id=TaskContext.get_run_id(), message=str(e))

                # Create a descriptive error message
                error_msg = f"Error in {func.__name__}: {e}"
                raise Exception(error_msg) from e

        return wrapper
    return decorator


def extract_site_and_date(site_to_process: tuple[str, str]) -> tuple[str, str]:
    """
    Helper to unpack site_to_process tuples.

    Args:
        site_to_process: Tuple of (site, delivery_date)

    Returns:
        Tuple of (site, delivery_date)
    """
    return site_to_process


# Re-export logging utility functions from utils module
# These are defined in utils.py to avoid circular imports
format_log_context = utils.format_log_context
extract_context_from_file_config = utils.extract_context_from_file_config
extract_context_from_site_tuple = utils.extract_context_from_site_tuple
