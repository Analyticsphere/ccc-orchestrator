import os
from pathlib import Path

from dependencies.ehr import constants, utils


class FileConfig:
    def __init__(self, site: str, delivery_date: str, source_file: str):
        self.site = site
        self.site_config = utils.get_site_config(site=site)
        self.source_file = source_file
        self.delivery_date = delivery_date
        self.project_id = self.site_config[constants.FileConfig.PROJECT_ID.value]
        self.gcs_bucket = self.site_config[constants.FileConfig.GCS_BUCKET.value]
        self.file_delivery_format = self.site_config[constants.FileConfig.FILE_DELIVERY_FORMAT.value]
        self.bq_dataset = self.site_config[constants.FileConfig.BQ_DATASET.value]
        self.omop_version = self.site_config[constants.FileConfig.OMOP_VERSION.value]
        self.file_path = f"{self.gcs_bucket}/{self.delivery_date}/{self.source_file}"
        self.date_format = self.site_config.get(constants.FileConfig.DATE_FORMAT.value, None)
        self.datetime_format = self.site_config.get(constants.FileConfig.DATETIME_FORMAT.value, None)
        self.overwrite_site_vocab_with_standard = self.site_config.get(constants.FileConfig.OVERWRITE_SITE_VOCAB_WITH_STANDARD.value)
        # Remove all file extensions (e.g., .csv.gz) to get the true base name for table_name
        table_base = Path(os.path.basename(source_file))
        while table_base.suffix:
            table_base = table_base.with_suffix("")
        self.table_name = table_base.name

    def to_dict(self):
        return {
            constants.FileConfig.SITE.value: self.site,
            constants.FileConfig.SOURCE_FILE.value: self.source_file,
            constants.FileConfig.DELIVERY_DATE.value: self.delivery_date,
            constants.FileConfig.FILE_DELIVERY_FORMAT.value: self.file_delivery_format,
            constants.FileConfig.PROJECT_ID.value: self.project_id,
            constants.FileConfig.GCS_BUCKET.value: self.gcs_bucket,
            constants.FileConfig.BQ_DATASET.value: self.bq_dataset,
            constants.FileConfig.OMOP_VERSION.value: self.omop_version,
            constants.FileConfig.FILE_PATH.value: self.file_path,
            constants.FileConfig.DATE_FORMAT.value: self.date_format,
            constants.FileConfig.DATETIME_FORMAT.value: self.datetime_format,
            constants.FileConfig.OVERWRITE_SITE_VOCAB_WITH_STANDARD.value: self.overwrite_site_vocab_with_standard,
            constants.FileConfig.TABLE_NAME.value: self.table_name
        }

    @classmethod
    def from_dict(cls, config_dict: dict) -> 'FileConfig':
        """
        Reconstruct a FileConfig object from a dictionary.

        This is useful when Airflow passes serialized file configs between tasks.

        Args:
            config_dict: Dictionary containing file configuration (from to_dict())

        Returns:
            FileConfig object with all properties populated
        """
        # Create instance using the basic constructor parameters
        instance = cls(
            site=config_dict[constants.FileConfig.SITE.value],
            delivery_date=config_dict[constants.FileConfig.DELIVERY_DATE.value],
            source_file=config_dict[constants.FileConfig.SOURCE_FILE.value]
        )
        return instance

    def __repr__(self):
        return str(self.to_dict())