variable "project_id" {
  description = "GCP project ID for the local-mode deployment."
  type        = string
}

variable "region" {
  description = "Default GCP region."
  type        = string
  default     = "europe-west2"
}

variable "bucket_name" {
  description = "Name of the GCS bucket used for raw and prepared files."
  type        = string
}

variable "raw_dataset_id" {
  description = "BigQuery raw dataset ID."
  type        = string
  default     = "uk_ev_raw"
}

variable "analytics_dataset_id" {
  description = "BigQuery analytics dataset ID."
  type        = string
  default     = "uk_ev_analytics"
}

variable "bucket_location" {
  description = "Location for the GCS bucket."
  type        = string
  default     = "EU"
}

variable "bigquery_location" {
  description = "Location for BigQuery datasets."
  type        = string
  default     = "EU"
}
