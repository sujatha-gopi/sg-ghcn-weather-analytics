terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  credentials = var.my_credentials
  project = var.project_id
  region  = var.region
}

# GCS bucket — our data lake for raw GHCN CSV files
resource "google_storage_bucket" "ghcn_datalake" {
  name          = var.gcs_bucket_name
  location      = var.region
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }
  uniform_bucket_level_access = true
}

resource "google_bigquery_dataset" "ghcn_warehouse" {
  dataset_id    = var.bq_dataset_name
  friendly_name = "GHCN Weather Warehouse"
  description   = "Data warehouse for NOAA GHCN-Daily global weather analysis"
  location      = var.region

  delete_contents_on_destroy = true
}
