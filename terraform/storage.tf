resource "google_storage_bucket" "uk_ev_pipeline_bucket" {
  name                        = var.bucket_name
  location                    = var.bucket_location
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = true

  depends_on = [
    google_project_service.storage
  ]
}
