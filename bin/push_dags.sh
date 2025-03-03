#!/bin/bash
# push_dags.sh: Pushes Airflow DAG files to your *dev* Composer environment.

set -e

# Configure for dev environment
PROJECT_ID="nih-nci-dceg-connect-dev"
#COMPOSER_BUCKET="us-central1-ccc-jp-dev-54b9b374-bucket"
COMPOSER_BUCKET="us-central1-ccc-pr2-jp-dev-bf0b32b4-bucket"

echo "Setting GCP project to: $PROJECT_ID"
gcloud config set project "$PROJECT_ID"

echo "Copying local DAGs to dev Composer bucket..."
gsutil -m cp -r dags/* gs://"$COMPOSER_BUCKET"/dags/

echo "Done. DAGs have been pushed to dev environment!"
 
