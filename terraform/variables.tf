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

variable "cloud_mode_enabled" {
  description = "Whether to provision the cloud-mode VM."
  type        = bool
  default     = false
}

variable "zone" {
  description = "GCP zone for the cloud VM."
  type        = string
  default     = "europe-west2-a"
}

variable "vm_name" {
  description = "Name of the cloud VM."
  type        = string
  default     = "uk-ev-pipeline-vm"
}

variable "machine_type" {
  description = "Machine type for the cloud VM."
  type        = string
  default     = "e2-standard-2"
}

variable "allowed_ingress_cidrs" {
  description = "CIDR ranges allowed to access the cloud VM."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "repo_url" {
  description = "Git repo URL to clone on the cloud VM."
  type        = string
}

variable "repo_branch" {
  description = "Git branch to clone on the cloud VM."
  type        = string
  default     = "main"
}

variable "bank_of_england_eur_gbp_fx_url" {
  description = "Source URL for the Bank of England FX dataset."
  type        = string
}

variable "european_wholesale_electricity_prices_url" {
  description = "Source URL for the European wholesale electricity dataset."
  type        = string
}

variable "desnz_petroleum_products_prices_url" {
  description = "Source URL for the DESNZ petroleum dataset."
  type        = string
}

variable "dvla_veh1103_url" {
  description = "Source URL for DVLA VEH1103."
  type        = string
}

variable "dvla_veh1153_url" {
  description = "Source URL for DVLA VEH1153."
  type        = string
}

variable "kestra_admin_email" {
  description = "Kestra admin email for cloud mode."
  type        = string
  default     = "kestra@kestra.com"
}

variable "kestra_admin_password" {
  description = "Kestra admin password for cloud mode."
  type        = string
  sensitive   = true
}

variable "kestra_admin_name" {
  description = "Kestra admin display name for cloud mode."
  type        = string
  default     = "kestra@kestra.com"
}
