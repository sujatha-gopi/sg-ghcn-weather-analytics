output "gcs_bucket_name" {
  description = "Name of the created GCS bucket"
  value       = google_storage_bucket.ghcn_datalake.name
}

output "bigquery_dataset" {
  description = "Name of the created BigQuery dataset"
  value       = google_bigquery_dataset.ghcn_warehouse.dataset_id
}
