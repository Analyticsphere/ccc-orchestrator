# OMOP/EHR Data Pipeline

An Apache Airflow pipeline for processing Electronic Health Record (EHR) data conforming to the OMOP Common Data Model (CDM). This pipeline automates ingestion, validation, CDM upgrade, vocabulary harmonization, and loading into BigQuery.

## Overview

Designed for Google Cloud Composer (managed Airflow), the DAG orchestrates the following:

1. Discovers the latest date-based deliveries in per-site GCS buckets
2. Converts incoming CSV/CSV.GZ files to optimized Parquet
3. Validates files against OMOP CDM schema
4. Normalizes data types/formats
5. Upgrades CDM versions if needed (e.g., 5.3 → 5.4)
6. Harmonizes vocabularies across clinical tables (multi-step)
7. Loads harmonized and remaining tables to BigQuery
8. Generates derived data tables and a delivery report; completes CDM metadata

The DAG id is `ehr-pipeline`.

## Prerequisites

- Google Cloud Platform with:
  - Cloud Composer environment
  - BigQuery access
  - Cloud Storage buckets per site, with deliveries organized as YYYY-MM-DD top-level folders
- OMOP processor API endpoint accessible from Composer
- OMOP vocabulary files available in GCS (see Environment Variables)
- Site configuration YAML uploaded with the DAGs (see Configuration)

Permissions/auth:
- Composer worker service account must be able to:
  - Obtain an identity token via gcloud and invoke the Processor API
  - Read from site GCS buckets and write artifacts
  - Create/read/write BigQuery datasets/tables

Note: Authentication uses an identity token from `gcloud auth print-identity-token`, which is available in Composer images. Ensure the API accepts Google-signed identity tokens (typical for Cloud Run with authenticated invokers).

## Configuration

### Site Configuration

Sites are configured in `dags/dependencies/ehr/config/site_config.yml`, stored in GCS.

Example:

```yaml
site:
  site_name:
    display_name: "Site Display Name"
    gcs_bucket: "my-site-bucket"           # bucket name only (no gs://)
    file_delivery_format: ".csv"           # or .csv.gz
    project_id: "gcp-project-id"
    bq_dataset: "bigquery_dataset"
    omop_version: "5.3"                    # delivered CDM version
    date_format: "%Y-%m-%d"                # date format used by site
    datetime_format: "%Y-%m-%d %H:%M:%S"   # datetime format used by site
    overwrite_site_vocab_with_standard: true # default true; if false, site vocab overwrites standard
```

### Environment Variables

Set as environment variables in Composer. Defaults shown are from `constants.py` but should be customized for your environment:

- OMOP_PROCESSOR_ENDPOINT: Required. Base URL for the Processor API (e.g., https://<cloud-run-service-url>)
- OMOP_ANALYZER_ENDPOINT: Optional. Analyzer API base URL (reserved)
- OMOP_TARGET_VOCAB_VERSION: Required. Target vocabulary version string (e.g., "v5.0 27-AUG-25")
- OMOP_TARGET_CDM_VERSION: Required. Target OMOP CDM version (e.g., "5.4")
- OMOP_VOCAB_GCS_PATH: Required. GCS path to vocab distribution (e.g., gs://<bucket>/vocab/<version>)

The site config file path is fixed in code to `/home/airflow/gcs/dags/dependencies/ehr/config/site_config.yml`.

## Pipeline Tasks

Main tasks and groups in `ehr_pipeline.py`:

- check_api_health: Verify Processor API availability
- id_sites_to_process: Identify sites with unprocessed/errored deliveries and generate optimized vocab if needed
- end_if_all_processed: Short-circuit when no sites to process
- get_unprocessed_files: Build list of files to process and log pipeline start per site
- convert_file: Convert CSV/CSV.GZ to Parquet
- validate_file: Validate against OMOP CDM schema
- normalize_file: Standardize data types and formats
- cdm_upgrade: Upgrade delivered CDM version to target (e.g., 5.3 → 5.4)

Vocabulary harmonization (TaskGroup: vocab_harmonization):
1. harmonize_vocab_source_target: Map source concepts to updated target codes
2. harmonize_vocab_target_remap: Remap non-standard targets to new standard targets
3. harmonize_vocab_target_replacement: Replace non-standard targets with new standard targets
4. harmonize_vocab_domain_check: Check and update domains as needed
5. harmonize_vocab_omop_etl: OMOP→OMOP ETL per table
6. harmonize_vocab_consolidate: Consolidate ETL outputs per site
7. harmonize_vocab_primary_keys_dedup: Deduplicate ETL primary keys per site

Load to BigQuery (TaskGroup: load_to_bigquery):
- prepare_bq: Clear prior tables in dataset
- load_harmonized_tables: Load harmonized ETL outputs
- load_target_vocab: Load standard vocab tables (skipped if site overrides)
- load_remaining: Load remaining OMOP tables from Parquet

Post-load:
- derived_data_tables: Populate observation_period, condition_era, drug_era
- final_cleanup: Generate delivery report, create any missing OMOP tables, populate cdm_source, and mark complete
- log_done: Final guard that marks the DAG failed if any prior tasks failed (and logs a DAG failure entry)

## Error Handling and Logging

- Automatic retries with exponential backoff for transient failures
- Centralized API error handling with contextual messages
- Pipeline phase logging via Processor API endpoints (`pipeline_log`, `get_log_row`)
- Airflow task logs and stdout (captured by Cloud Logging)
- `log_done` inspects task states and fails the run if any task failed

Note: The logging BigQuery dataset/table is managed by the Processor API; this DAG does not configure the table name directly.

## File Structure

```
dags/
├── ehr_pipeline.py                 # Main DAG
└── dependencies/
    └── ehr/
        ├── bq.py                  # BigQuery operations and pipeline logging calls
        ├── constants.py           # Env vars, enums, constants
        ├── file_config.py         # Per-file config builder
        ├── omop.py                # OMOP-specific API calls (upgrade, reports, CDM tables)
        ├── processing.py          # File discovery, conversion, normalization
        ├── utils.py               # Logging, auth, site config, helpers
        ├── validation.py          # Schema validation, delivery report
        ├── vocab.py               # Vocabulary loading and harmonization steps
        └── config/
            └── site_config.yml    # Site configuration (checked into Composer DAGs bucket)
```

## Deployment (Cloud Composer)

1. Upload this repository’s `dags/` folder (preserving subpaths) to your Composer environment’s DAGs bucket.
2. Create Composer environment variables for the values listed above (OMOP_... variables).
3. Place `site_config.yml` at `dags/dependencies/ehr/config/site_config.yml` in the DAGs bucket.
4. Grant Composer’s service account permissions to:
   - Invoke the Processor API (Cloud Run Invoker or equivalent)
   - Read/write the site GCS buckets and artifacts
   - Read/write the target BigQuery datasets
5. In Airflow UI, trigger the `ehr-pipeline` DAG.

Optional quick checks:
- Ensure each site bucket has a top-level folder named like `YYYY-MM-DD` containing OMOP CSV/CSV.GZ files.
- Verify the Processor API heartbeat at `GET {OMOP_PROCESSOR_ENDPOINT}/heartbeat` returns `{ status: "healthy" }` for the Composer service account.

## Notes and Tips

- Harmonization applies only to clinical tables listed in `constants.VOCAB_HARMONIZED_TABLES`.
- If `overwrite_site_vocab_with_standard` is false, the site-provided vocab tables will overwrite standard vocab tables loaded by the pipeline.
- GCS bucket names in config should not include `gs://`; the pipeline will add URI prefixes where needed.
- The target vocabulary files should be present under `OMOP_VOCAB_GCS_PATH` and identified by `OMOP_TARGET_VOCAB_VERSION`.

## Development

### Adding New Sites

1. Add the site under `site_config.yml` using the schema above.
2. Grant Composer read/write access to the site’s GCS bucket.
3. Create the target BigQuery dataset (or permissions to create it).
4. Test with a small delivery under `gs://<bucket>/YYYY-MM-DD/`.

### Extending the Pipeline

- To add new derived tables, update `constants.DERIVED_DATA_TABLES` and extend `omop.create_derived_data_table` handling in the Processor API.
- To adjust harmonization, modify steps in `vocab.py` or the sequence in the `vocab_harmonization` TaskGroup.