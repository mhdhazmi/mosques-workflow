provider "google" {
  project = var.project_id
  region  = var.region
}

# --- Variables ---
variable "project_id" {
  description = "The ID of the Google Cloud project"
  type        = string
}

variable "region" {
  description = "GCP Region (e.g., me-central2)"
  type        = string
  default     = "me-central2"
}

variable "bucket_name" {
  description = "Name of the GCS bucket (must be globally unique)"
  type        = string
}

variable "dataset_id" {
  description = "BigQuery Dataset ID"
  type        = string
  default     = "raw_meter_readings"
}

# --- 1. The Landing Zone (GCS Bucket) ---
resource "google_storage_bucket" "data_lake" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true 
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "Delete"
    }
  }
}

# --- 2. The Warehouse (BigQuery Dataset) ---
resource "google_bigquery_dataset" "meter_readings" {
  dataset_id    = var.dataset_id
  friendly_name = "Raw Meter Data"
  description   = "Ingested parquet files from local pipeline"
  location      = var.region
}

# --- Outputs ---
output "bucket_name" {
  value = google_storage_bucket.data_lake.name
}

output "dataset_id" {
  value = google_bigquery_dataset.meter_readings.dataset_id
}
