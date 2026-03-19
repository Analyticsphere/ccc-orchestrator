# EHR Pipeline

Airflow DAG for processing OMOP EHR deliveries in Cloud Composer.

The DAG id is `ehr-pipeline`.

## Purpose

For each site delivery, the pipeline:

1. Finds the most recent delivery folder in the site's GCS bucket.
2. Exports Connect reference data for the site.
3. Converts source files to Parquet.
4. Validates and normalizes OMOP files.
5. Upgrades delivered CDM versions when required.
6. Filters participants using Connect status data.
7. Runs vocabulary harmonization for supported clinical tables.
8. Generates derived tables.
9. Loads the processed data to BigQuery.
10. Runs DQD, Achilles, PASS, and delivery reporting.

## Runtime Model

This repository contains the Composer DAG and task helpers. The heavy processing is executed by external services:

- `OMOP_PROCESSOR_ENDPOINT` handles file conversion, validation, normalization, CDM upgrades, vocabulary work, BigQuery preparation, and related OMOP operations.
- `OMOP_ANALYZER_ENDPOINT` handles DQD, Achilles, Atlas table creation, and report generation.

The DAG coordinates site discovery, task ordering, retries, and pipeline logging.

## Requirements

- Google Cloud Composer
- BigQuery datasets for CDM and analytics results
- Per-site GCS buckets with deliveries stored as top-level `YYYY-MM-DD` folders
- Reachable processor and analyzer services
- Composer worker permissions to:
  - read and write the configured GCS buckets
  - invoke the processor and analyzer services
  - create, truncate, and load BigQuery tables

## Configuration

### Site Config

The DAG reads site configuration from:

`/home/airflow/gcs/dags/dependencies/ehr/config/site_config.yml`

Example:

```yaml
site:
  site_name:
    display_name: "Site Display Name"
    gcs_bucket: "my-site-bucket"
    file_delivery_format: ".csv"
    project_id: "gcp-project-id"
    cdm_bq_dataset: "omop_cdm"
    analytics_bq_dataset: "omop_analytics"
    omop_version: "5.3"
    date_format: "%Y-%m-%d"
    datetime_format: "%Y-%m-%d %H:%M:%S"
    overwrite_site_vocab_with_standard: true
    site_connect_id: 123456789
```

Fields used by the DAG:

- `display_name`: human-readable site name used in logs and reports
- `gcs_bucket`: bucket name only, without `gs://`
- `file_delivery_format`: expected source file format, typically `.csv` or `.csv.gz`
- `project_id`: GCP project for BigQuery operations
- `cdm_bq_dataset`: target OMOP dataset
- `analytics_bq_dataset`: target analytics dataset for DQD, Achilles, PASS, and Atlas outputs
- `omop_version`: delivered CDM version
- `date_format` and `datetime_format`: source formatting hints for normalization
- `overwrite_site_vocab_with_standard`: when `true`, standard vocabulary is loaded and site vocab tables are skipped
- `site_connect_id`: Connect identifier used for site-level participant export

### Environment Variables

Defined in [dags/dependencies/ehr/constants.py](/Users/frankenbergerea/Development/ccc-orchestrator/dags/dependencies/ehr/constants.py):

- `OMOP_PROCESSOR_ENDPOINT`
- `OMOP_ANALYZER_ENDPOINT`
- `OMOP_TARGET_VOCAB_VERSION`
- `OMOP_TARGET_CDM_VERSION`
- `OMOP_VOCAB_GCS_PATH`
- `CONNECT_DATASET_ID`

## DAG Flow

The main flow in [dags/ehr_pipeline.py](/Users/frankenbergerea/Development/ccc-orchestrator/dags/ehr_pipeline.py) is:

1. `check_api_health`
2. `id_sites_to_process`
3. `get_unprocessed_files`
4. Per-site `retrieve_connect_data`
5. Per-file `convert_file`, `validate_file`, `normalize_file`, `cdm_upgrade`, `filter_participants`
6. Per-site `populate_cdm_source_file`
7. `vocab_harmonization` task group
8. `generate_derived_tables` task group
9. `load_to_bigquery` task group
10. `cleanup`
11. Per-site `generate_report_csv`
12. Per-site `dqd`, `achilles`, `pass_analysis`
13. Per-site `atlas_results_tables` and `generate_delivery_report`
14. `mark_delivery_complete`
15. `log_done`

### Vocabulary Harmonization

The `vocab_harmonization` task group runs these steps in order:

1. `harmonize_vocab_source_target`
2. `harmonize_vocab_target_remap`
3. `harmonize_vocab_target_replacement`
4. `harmonize_vocab_domain_check`
5. `harmonize_vocab_omop_etl`
6. `harmonize_vocab_consolidate`
7. `harmonize_vocab_discover_tables`
8. `flatten_table_configs`
9. `harmonize_vocab_deduplicate_table`

Only tables listed in `VOCAB_HARMONIZED_TABLES` are processed by these steps.

### Derived Tables

Derived tables are generated after vocabulary harmonization and before BigQuery loading.

The current derived tables are defined by `DERIVED_DATA_TABLES` in [dags/dependencies/ehr/constants.py](/Users/frankenbergerea/Development/ccc-orchestrator/dags/dependencies/ehr/constants.py).

## Deployment

1. Upload `dags/` to the Composer DAGs bucket, preserving paths.
2. Ensure `site_config.yml` is present at the configured path in the DAGs bucket.
3. Set the required environment variables in Composer.
4. Grant Composer access to the configured GCS buckets, BigQuery datasets, and service endpoints.
5. Trigger `ehr-pipeline` from Airflow.

## Operational Notes

- A site is processed when its latest delivery has no log row or its most recent log row is in an error state.
- `load_harmonized_tables` skips cleanly when no harmonized clinical tables were produced.
- `load_remaining` skips vocabulary tables already handled elsewhere and skips `cdm_source`, which is loaded during `cleanup`.
- `log_done` inspects task states and fails the DAG if any upstream task failed, even if a mapped task failure was otherwise easy to miss.

## Repository Layout

```text
dags/
  ehr_pipeline.py
  dependencies/
    ehr/
      analysis.py
      bq.py
      constants.py
      dag_helpers.py
      file_config.py
      omop.py
      participant_filter.py
      processing.py
      processing_jobs.py
      storage_backend.py
      utils.py
      validation.py
      vocab.py
      config/
        site_config.yml
```
