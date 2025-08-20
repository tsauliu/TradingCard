# GCP BigQuery Setup Guide for TCG Data Pipeline

Complete setup guide for Google Cloud Platform integration with the TCG CSV data pipeline.

**Project ID:** `rising-environs-456314-a3`  
**Region:** `asia-east1`

## Prerequisites

1. **Install Google Cloud SDK**
   ```bash
   # macOS
   brew install google-cloud-sdk
   
   # Ubuntu/Debian
   sudo apt-get update
   sudo apt-get install google-cloud-cli
   
   # Or download from: https://cloud.google.com/sdk/docs/install
   ```

2. **Authenticate with Google Cloud**
   ```bash
   gcloud auth login
   ```

## Step 1: Configure Default Project and Region

```bash
# Set your default project
gcloud config set project rising-environs-456314-a3

# Set default region
gcloud config set compute/region asia-east1

# Verify configuration
gcloud config list
```

## Step 2: Enable Required APIs

```bash
# Enable BigQuery API
gcloud services enable bigquery.googleapis.com

# Enable BigQuery Storage API (for better performance)
gcloud services enable bigquerystorage.googleapis.com

# Verify APIs are enabled
gcloud services list --enabled | grep bigquery
```

## Step 3: Create Service Account

```bash
# Create service account for the TCG pipeline
gcloud iam service-accounts create tcg-bigquery-sa \
    --display-name="TCG BigQuery Service Account" \
    --description="Service account for TCG data pipeline to BigQuery"

# Verify service account creation
gcloud iam service-accounts list | grep tcg-bigquery-sa
```

## Step 4: Grant BigQuery Permissions

```bash
# Grant BigQuery Data Editor role (create/modify datasets and tables)
gcloud projects add-iam-policy-binding rising-environs-456314-a3 \
    --member="serviceAccount:tcg-bigquery-sa@rising-environs-456314-a3.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataEditor"

# Grant BigQuery Job User role (run queries and jobs)
gcloud projects add-iam-policy-binding rising-environs-456314-a3 \
    --member="serviceAccount:tcg-bigquery-sa@rising-environs-456314-a3.iam.gserviceaccount.com" \
    --role="roles/bigquery.jobUser"

# Verify permissions
gcloud projects get-iam-policy rising-environs-456314-a3 \
    --flatten="bindings[].members" \
    --filter="bindings.members:tcg-bigquery-sa@rising-environs-456314-a3.iam.gserviceaccount.com"
```

## Step 5: Download Service Account Key

### Option A: Using gcloud CLI (Recommended)
```bash
# Create and download JSON key file
gcloud iam service-accounts keys create service-account.json \
    --iam-account=tcg-bigquery-sa@rising-environs-456314-a3.iam.gserviceaccount.com

# Verify the file was created
ls -la service-account.json
```

### Option B: Using GCP Console
1. Go to [GCP Console](https://console.cloud.google.com)
2. Navigate to **IAM & Admin** → **Service Accounts**
3. Find `tcg-bigquery-sa@rising-environs-456314-a3.iam.gserviceaccount.com`
4. Click the three dots (⋮) → **Manage keys**
5. Click **ADD KEY** → **Create new key**
6. Select **JSON** format and click **CREATE**
7. Save the downloaded file as `service-account.json`

## Step 6: Create BigQuery Dataset (Optional)

The Python script will create the dataset automatically, but you can create it manually:

```bash
# Install BigQuery CLI if not already installed
sudo apt-get install google-cloud-cli-bq

# Create dataset in asia-east1 region
bq mk --location=asia-east1 --dataset rising-environs-456314-a3:tcg_data

# Verify dataset creation
bq ls --dataset_id=rising-environs-456314-a3:tcg_data
```

## Step 7: Setup Local Environment

```bash
# Copy environment template
cp .env.example .env

# The .env file should contain:
# GOOGLE_CLOUD_PROJECT=rising-environs-456314-a3
# GOOGLE_APPLICATION_CREDENTIALS=service-account.json
# BIGQUERY_DATASET=tcg_data

# Install Python dependencies
pip install -r requirements.txt
```

## Step 8: Verify Setup

```bash
# Test gcloud authentication
gcloud auth application-default print-access-token

# Test BigQuery access
bq query --use_legacy_sql=false 'SELECT 1 as test'

# Run the TCG pipeline
python main.py
```

## Security Best Practices

1. **Protect Service Account Key**
   ```bash
   # Set proper file permissions
   chmod 600 service-account.json
   
   # Add to .gitignore
   echo "service-account.json" >> .gitignore
   echo ".env" >> .gitignore
   ```

2. **Use IAM Conditions** (Optional - for advanced users)
   ```bash
   # Example: Restrict access to specific dataset
   gcloud projects add-iam-policy-binding rising-environs-456314-a3 \
       --member="serviceAccount:tcg-bigquery-sa@rising-environs-456314-a3.iam.gserviceaccount.com" \
       --role="roles/bigquery.dataEditor" \
       --condition='expression=resource.name.startsWith("projects/rising-environs-456314-a3/datasets/tcg_data"),title=TCG Dataset Only'
   ```

## Troubleshooting

### Common Issues

1. **Authentication Error**
   ```bash
   # Re-authenticate
   gcloud auth login
   gcloud auth application-default login
   ```

2. **Permission Denied**
   ```bash
   # Check IAM permissions
   gcloud projects get-iam-policy rising-environs-456314-a3
   ```

3. **API Not Enabled**
   ```bash
   # List enabled services
   gcloud services list --enabled
   
   # Enable missing APIs
   gcloud services enable bigquery.googleapis.com
   ```

4. **Service Account Key Issues**
   ```bash
   # List existing keys
   gcloud iam service-accounts keys list \
       --iam-account=tcg-bigquery-sa@rising-environs-456314-a3.iam.gserviceaccount.com
   
   # Create new key if needed
   gcloud iam service-accounts keys create new-service-account.json \
       --iam-account=tcg-bigquery-sa@rising-environs-456314-a3.iam.gserviceaccount.com
   ```

5. **Dataset Location Issues**
   ```bash
   # Check dataset location
   bq show --format=prettyjson rising-environs-456314-a3:tcg_data
   ```

## Cleanup Commands (If Needed)

```bash
# Delete service account key
gcloud iam service-accounts keys delete KEY_ID \
    --iam-account=tcg-bigquery-sa@rising-environs-456314-a3.iam.gserviceaccount.com

# Delete service account
gcloud iam service-accounts delete \
    tcg-bigquery-sa@rising-environs-456314-a3.iam.gserviceaccount.com

# Delete dataset
bq rm -r -d rising-environs-456314-a3:tcg_data
```

---

**Next Steps:** After completing this setup, run `python main.py` to test the complete TCG data pipeline!