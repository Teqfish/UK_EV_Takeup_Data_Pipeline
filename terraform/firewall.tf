resource "google_compute_firewall" "cloud_vm_ssh" {
  count   = var.cloud_mode_enabled ? 1 : 0
  name    = "${var.vm_name}-allow-ssh"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = var.allowed_ingress_cidrs
  target_tags   = ["uk-ev-cloud-vm"]
}

resource "google_compute_firewall" "cloud_vm_apps" {
  count   = var.cloud_mode_enabled ? 1 : 0
  name    = "${var.vm_name}-allow-apps"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["8082", "8502"]
  }

  source_ranges = var.allowed_ingress_cidrs
  target_tags   = ["uk-ev-cloud-vm"]
}
