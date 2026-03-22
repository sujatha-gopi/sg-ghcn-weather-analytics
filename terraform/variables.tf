variable "my_credentials"{
    description = "GCP Credentials"
    default = "ghcn-sa-key.json"
}

variable "project_id"{
    description = "GCP project ID"
    type        = string
    default     = "mythic-altar-485103-v8"
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "gcs_bucket_name"{
    description = "GCS bucket for raw GHCN data lake"
    type        = string
    default     = "ghcn-weather-datalake-mythic-altar-485103-v8"
}

variable "bq_dataset_name"{
    description = "BigQuery dataset for GHCN warehouse"
    type        = string
    default     = "ghcn_weather"
}