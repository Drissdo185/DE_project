
terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "4.80.0"
    }
  }
  required_version = "1.7.5"
}

provider "google" {
  project     = var.project_id
  region      = var.region
}

// Google Kubernetes Engine
resource "google_container_cluster" "primary" {
  name     = "${var.project_id}-gke"
  location = var.region

  remove_default_node_pool = true
  initial_node_count       = 1

}

resource "google_container_node_pool" "primary_preemptible_nodes" {
  name       = "node-pool"
  location   = var.region
  cluster    = google_container_cluster.primary.name
  node_count = 1

  node_config {
    preemptible  = true
    machine_type = "e2-standard-8" # 8 CPU and 32 GB Memory
  }
}

resource "google_storage_bucket" "bucket" {
  name     = "${var.project_id}-bucket"
  location = var.region
  force_destroy = true  // Set to false if you want to prevent Terraform from deleting the bucket
}
