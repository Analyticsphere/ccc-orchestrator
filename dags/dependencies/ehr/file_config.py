from dependencies.ehr import constants, utils


class FileConfig:
    def __init__(self, site: str, file_name: str):
        self.site = site
        self.site_config = utils.get_site_config_file()[constants.FileConfig.SITE.value][site]
        self.file_name = utils.remove_date_prefix(file_name)
        self.delivery_date = utils.get_date_prefix(file_name)
        self.project_id = self.site_config[constants.FileConfig.PROJECT_ID.value]
        self.gcs_path = self.site_config[constants.FileConfig.GCS_PATH.value]
        self.file_delivery_format = self.site_config[constants.FileConfig.FILE_DELIVERY_FORMAT.value]
        self.bq_dataset = self.site_config[constants.FileConfig.BQ_DATASET.value]
        self.omop_version = self.site_config[constants.FileConfig.OMOP_VERSION.value]

    def to_dict(self):
        return {
            constants.FileConfig.SITE.value: self.site,
            constants.FileConfig.FILE_NAME.value: self.file_name,
            constants.FileConfig.DELIVERY_DATE.value: self.delivery_date,
            constants.FileConfig.FILE_DELIVERY_FORMAT.value: self.file_delivery_format,
            constants.FileConfig.PROJECT_ID.value: self.project_id,
            constants.FileConfig.GCS_PATH.value: self.gcs_path,
            constants.FileConfig.BQ_DATASET.value: self.bq_dataset,
            constants.FileConfig.OMOP_VERSION.value: self.omop_version
        }

    def __repr__(self):
        return str(self.to_dict())