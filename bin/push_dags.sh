#!/bin/bash
# push_dags.sh: Pushes Airflow DAG files to your *dev* Composer environment.

set -e

# Configure for dev environment
PROJECT_ID="nih-nci-dceg-connect-dev"

# Determine COMPOSER_BUCKET from a command-line argument or environment variable.
if [ -n "$1" ]; then
    COMPOSER_BUCKET="$1"
elif [ -n "$COMPOSER_BUCKET" ]; then
    COMPOSER_BUCKET="$COMPOSER_BUCKET"
else
    echo "Error: COMPOSER_BUCKET not specified. Provide it as an argument or set the COMPOSER_BUCKET environment variable."
    exit 1
fi

echo "Setting GCP project to: $PROJECT_ID"
gcloud config set project "$PROJECT_ID"

echo "Using Composer bucket: $COMPOSER_BUCKET"
echo "Copying local DAGs to Composer bucket..."
gsutil -m cp -r dags/* gs://"$COMPOSER_BUCKET"/dags/

echo "Done. DAGs have been pushed to the environment!"
