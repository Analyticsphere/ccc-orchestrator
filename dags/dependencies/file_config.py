from . import utils

class FileConfig:
    def __init__(self, site: str, file_name: str):
        self.site = site
        self.site_config = utils.get_site_config_file()['site'][site]
        self.file_name = file_name
        #self.file_path = TODO: Create fully qualified GCS path?
        self.project_id = self.site_config['project_id']
        self.gcs_path = self.site_config['gcs_path']
        self.bq_table = self.site_config['bq_omop_table']
        self.omop_version = self.site_config['omop_version']