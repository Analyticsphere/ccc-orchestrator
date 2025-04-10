import os

from dependencies.ehr import constants, utils


class FileConfig:
    def __init__(self, site: str, delivery_date: str, source_file: str):
        self.site = site
        self.site_config = utils.get_site_config_file()[constants.FileConfig.SITE.value][site]
        self.source_file = source_file
        self.delivery_date = delivery_date
        self.project_id = self.site_config[constants.FileConfig.PROJECT_ID.value]
        self.gcs_bucket = self.site_config[constants.FileConfig.GCS_BUCKET.value]
        self.file_delivery_format = self.site_config[constants.FileConfig.FILE_DELIVERY_FORMAT.value]
        self.bq_dataset = self.site_config[constants.FileConfig.BQ_DATASET.value]
        self.omop_version = self.site_config[constants.FileConfig.OMOP_VERSION.value]
        self.file_path = f"{self.gcs_bucket}/{self.delivery_date}/{self.source_file}"
        self.table_name = os.path.splitext(os.path.basename(source_file))[0]

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
            constants.FileConfig.TABLE_NAME.value: self.table_name
        }

    def __repr__(self):
        return str(self.to_dict())