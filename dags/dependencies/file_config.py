from . import utils

class FileConfig:
    def __init__(self, site: str, file_name: str):
        self.site = site
        self.site_config = utils.get_site_config_file()['site'][site]
        self.file_name = utils.remove_date_prefix(file_name)
        self.delivery_date = utils.get_date_prefix(file_name)
        self.project_id = self.site_config['project_id']
        self.gcs_path = self.site_config['gcs_path']
        self.bq_table = self.site_config['bq_omop_table']
        self.omop_version = self.site_config['omop_version']

    def to_dict(self):
        return {
            'site': self.site,
            'file_name': self.file_name,
            'delivery_date': self.delivery_date,
            'project_id': self.project_id,
            'gcs_path': self.gcs_path,
            'bq_table': self.bq_table,
            'omop_version': self.omop_version
        }

    def __repr__(self):
        return str(self.to_dict())