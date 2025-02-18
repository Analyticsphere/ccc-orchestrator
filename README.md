# OMOP/EHR Data Pipeline

An Apache Airflow pipeline for processing Electronic Health Record (EHR) data conforming to the OMOP Common Data Model (CDM). This pipeline automates the ingestion, validation, and transformation of OMOP-formatted CSV files into BigQuery tables.

## Overview

This pipeline is designed to run on Google Cloud Composer (managed Airflow) and handles the following workflow:

1. Monitors GCS buckets for new OMOP data deliveries from various sites
2. Processes CSV files into optimized Parquet format
3. Validates data against OMOP CDM specifications
4. Normalizes and standardizes data structure
5. Upgrades CDM version if needed (supports 5.3 → 5.4)
6. Loads processed data into BigQuery tables

## Prerequisites

- Google Cloud Platform account with:
  - Cloud Composer environment
  - BigQuery access
  - Cloud Storage buckets configured
- OMOP File Processor API endpoint configured and running
- Site configuration YAML file

## Configuration

### Site Configuration

Sites are configured in `site_config.yml`:

```yaml
site:
  'site_name':
    display_name: 'Site Display Name'
    gcs_path: 'gcs_bucket_name'
    file_delivery_format: '.csv'
    project_id: 'gcp-project-id'
    bq_dataset: 'bigquery_dataset'
    omop_version: '5.4'
    post_processing: ['cdm_upgrade']
```

### Environment Variables

The pipeline expects the following configuration:
- `PROCESSOR_ENDPOINT`: URL of the OMOP file processor API
- `SITE_CONFIG_YML_PATH`: Path to the site configuration YAML file
- `PIPELINE_LOG_TABLE`: BigQuery table for pipeline logging

## Pipeline Tasks

### Main Tasks

1. `check_api_health`: Verifies OMOP file processor API availability
2. `get_site_deliveries`: Identifies unprocessed site data deliveries
3. `get_files`: Creates list of files requiring processing
4. `process_file`: Converts CSV files to Parquet format
5. `validate_file`: Validates against OMOP CDM specifications
6. `normalize_file`: Standardizes data structure
7. `cdm_upgrade`: Performs CDM version upgrades if needed
8. `prepare_bq`: Prepares BigQuery environment
9. `load_to_bq`: Loads data into BigQuery tables
10. `final_cleanup`: Performs post-processing cleanup

### Error Handling

- All tasks include comprehensive error logging
- Pipeline status is tracked in BigQuery logging table
- Automatic retries configured for transient failures

## File Structure

```
dags/
├── ehr_pipeline.py       # Main DAG definition
├── config/
│   └── site_config.yml   # Site configuration
└── dependencies/
    ├── bq.py            # BigQuery operations
    ├── constants.py      # Configuration constants
    ├── file_config.py    # File configuration handler
    ├── processing.py     # File processing operations
    ├── utils.py         # Utility functions
    └── validation.py     # Data validation logic
```

## Logging

The pipeline maintains detailed logs in:
- Airflow task logs
- BigQuery logging table
- Standard output (captured by Cloud Logging)

Log entries include:
- Pipeline execution status
- Processing steps completion
- Error messages and stack traces
- Site and delivery information

## Error States

The pipeline recognizes the following states:
- `started`: Initial processing begun
- `running`: Active processing
- `completed`: Successfully finished
- `error`: Processing failed

## Development

### Adding New Sites

1. Add site configuration to `site_config.yml`
2. Ensure GCS bucket permissions are configured
3. Verify BigQuery dataset exists
4. Test with sample data delivery

## TODO: Production Deployment

1. Deploy to Cloud Composer
2. Configure service account permissions
3. Verify API endpoint accessibility
4. Test with production site configuration