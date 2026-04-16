output "project_id" {
  description = "GCP project ID."
  value       = var.project_id
}

output "bucket_name" {
  description = "Provisioned GCS bucket name."
  value       = google_storage_bucket.uk_ev_pipeline_bucket.name
}

output "raw_dataset_id" {
  description = "Raw BigQuery dataset ID."
  value       = google_bigquery_dataset.raw.dataset_id
}

output "analytics_dataset_id" {
  description = "Analytics BigQuery dataset ID."
  value       = google_bigquery_dataset.analytics.dataset_id
}

output "bigquery_location" {
  description = "BigQuery dataset location."
  value       = var.bigquery_location
}
