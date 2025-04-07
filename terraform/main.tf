# main.tf
provider "google" {
  credentials = file(var.credentials_file)
  project     = var.project_id
  region      = var.region
}

# Storage resources
# Note: The data lake bucket is now created manually via the create_public_bucket.py script
# and is not managed by Terraform anymore

# BigQuery datasets
resource "google_bigquery_dataset" "raw_data" {
  dataset_id = "raw_trade_data"
  location   = var.region
  description = "Raw international trade data"
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "processed_data" {
  dataset_id = "processed_trade_data"
  location   = var.region
  description = "Processed international trade data"
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id = "trade_analytics"
  location   = var.region
  description = "Trade analytics data models"
  delete_contents_on_destroy = true
}

# Service account for data pipeline
resource "google_service_account" "pipeline_service_account" {
  account_id   = "trade-data-pipeline"
  display_name = "Trade Data Pipeline Service Account"
  description  = "Service account for running data pipelines"
}

# IAM roles for service account
resource "google_project_iam_binding" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  members = [
    "serviceAccount:${google_service_account.pipeline_service_account.email}",
  ]
}

resource "google_project_iam_binding" "bigquery_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  members = [
    "serviceAccount:${google_service_account.pipeline_service_account.email}",
  ]
}

resource "google_compute_instance" "pipeline_vm" {
  name         = "pipeline-runner"
  machine_type = "e2-micro"  # 2 shared vCPUs, 1 GB memory
  zone         = "${var.region}-a"

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"  # Using Debian as base OS
      size  = 20  # Reduced size since we don't need container storage
    }
  }

  network_interface {
    network = "default"
    access_config {
      // Ephemeral IP
    }
  }

  service_account {
    email  = google_service_account.pipeline_service_account.email
    scopes = ["cloud-platform"]
  }

  # Basic metadata tags
  tags = ["pipeline"]

  # Allow HTTP access
  metadata = {
    enable-oslogin = "TRUE"
  }
}