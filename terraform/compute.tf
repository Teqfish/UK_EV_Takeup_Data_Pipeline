resource "google_compute_instance" "cloud_vm" {
  count        = var.cloud_mode_enabled ? 1 : 0
  name         = var.vm_name
  machine_type = var.machine_type
  zone         = var.zone
  tags         = ["uk-ev-cloud-vm"]

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
      size  = 30
      type  = "pd-balanced"
    }
  }

  network_interface {
    network = "default"

    access_config {
    }
  }

  service_account {
    email  = google_service_account.cloud_vm[0].email
    scopes = ["cloud-platform"]
  }

  metadata_startup_script = templatefile("${path.module}/scripts/startup.sh.tftpl", {
    repo_url                                  = var.repo_url
    repo_branch                               = var.repo_branch
    project_id                                = var.project_id
    bucket_name                               = var.bucket_name
    raw_dataset_id                            = var.raw_dataset_id
    analytics_dataset_id                      = var.analytics_dataset_id
    bank_of_england_eur_gbp_fx_url            = var.bank_of_england_eur_gbp_fx_url
    european_wholesale_electricity_prices_url = var.european_wholesale_electricity_prices_url
    desnz_petroleum_products_prices_url       = var.desnz_petroleum_products_prices_url
    dvla_veh1103_url                          = var.dvla_veh1103_url
    dvla_veh1153_url                          = var.dvla_veh1153_url
    kestra_admin_email                        = var.kestra_admin_email
    kestra_admin_password                     = var.kestra_admin_password
    kestra_admin_name                         = var.kestra_admin_name
    service_account_key_b64                   = google_service_account_key.cloud_vm_key[0].private_key
  })

  depends_on = [
    google_project_service.compute,
    google_project_service.iam,
    google_project_service.bigquery,
    google_project_service.storage,
    google_project_iam_member.cloud_vm_bigquery_admin,
    google_project_iam_member.cloud_vm_storage_admin,
  ]
}
