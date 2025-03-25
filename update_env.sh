#!/bin/bash

# Get the bucket name and project ID from Terraform output
cd terraform
BUCKET_NAME=$(terraform output -raw bucket_name)
PROJECT_ID=$(terraform output -raw project_id)
cd ..

# Update the .env file with the new bucket name and project ID
if [ -f ".env" ]; then
    # Update GCS_BUCKET_NAME
    if grep -q "GCS_BUCKET_NAME=" .env; then
        sed -i '' "s|GCS_BUCKET_NAME=.*|GCS_BUCKET_NAME=$BUCKET_NAME|g" .env
    else
        echo "GCS_BUCKET_NAME=$BUCKET_NAME" >> .env
    fi
    
    # Update GCP_PROJECT_ID
    if grep -q "GCP_PROJECT_ID=" .env; then
        sed -i '' "s|GCP_PROJECT_ID=.*|GCP_PROJECT_ID=$PROJECT_ID|g" .env
    else
        echo "GCP_PROJECT_ID=$PROJECT_ID" >> .env
    fi
    
    echo "Updated .env file with:"
    echo "  - Bucket name: $BUCKET_NAME"
    echo "  - Project ID: $PROJECT_ID"
else
    # Create .env file if it doesn't exist
    cp .env.template .env
    sed -i '' "s|GCS_BUCKET_NAME=.*|GCS_BUCKET_NAME=$BUCKET_NAME|g" .env
    sed -i '' "s|GCP_PROJECT_ID=.*|GCP_PROJECT_ID=$PROJECT_ID|g" .env
    echo "Created .env file with:"
    echo "  - Bucket name: $BUCKET_NAME"
    echo "  - Project ID: $PROJECT_ID"
fi

echo "Don't forget to update other environment variables in .env if needed!"
