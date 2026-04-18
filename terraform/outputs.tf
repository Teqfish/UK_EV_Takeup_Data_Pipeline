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

output "cloud_vm_external_ip" {
  description = "External IP of the cloud-mode VM."
  value       = var.cloud_mode_enabled ? google_compute_instance.cloud_vm[0].network_interface[0].access_config[0].nat_ip : null
}

output "cloud_kestra_url" {
  description = "Kestra URL for cloud mode."
  value       = var.cloud_mode_enabled ? "http://${google_compute_instance.cloud_vm[0].network_interface[0].access_config[0].nat_ip}:8080" : null
}

output "cloud_streamlit_url" {
  description = "Streamlit URL for cloud mode."
  value       = var.cloud_mode_enabled ? "http://${google_compute_instance.cloud_vm[0].network_interface[0].access_config[0].nat_ip}:8501" : null
}
