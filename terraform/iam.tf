resource "google_service_account" "cloud_vm" {
  count        = var.cloud_mode_enabled ? 1 : 0
  account_id   = "uk-ev-cloud-vm-sa"
  display_name = "UK EV Pipeline Cloud VM Service Account"
}

resource "google_project_iam_member" "cloud_vm_bigquery_admin" {
  count   = var.cloud_mode_enabled ? 1 : 0
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.cloud_vm[0].email}"
}

resource "google_project_iam_member" "cloud_vm_storage_admin" {
  count   = var.cloud_mode_enabled ? 1 : 0
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.cloud_vm[0].email}"
}

resource "google_service_account_key" "cloud_vm_key" {
  count              = var.cloud_mode_enabled ? 1 : 0
  service_account_id = google_service_account.cloud_vm[0].name
  private_key_type   = "TYPE_GOOGLE_CREDENTIALS_FILE"
}
