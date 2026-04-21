resource "google_bigquery_dataset" "raw" {
  dataset_id                 = var.raw_dataset_id
  location                   = var.bigquery_location
  delete_contents_on_destroy = true

  depends_on = [
    google_project_service.bigquery
  ]
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id                 = var.analytics_dataset_id
  location                   = var.bigquery_location
  delete_contents_on_destroy = true

  depends_on = [
    google_project_service.bigquery
  ]
}
