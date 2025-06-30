terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "6.41.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = "us-central1"
}

resource "google_storage_bucket" "auto-expire" {
  name          = var.name
  location      = "US"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 3
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id                  = var.name
  friendly_name               = "sample dataset"
  location                    = "US"
  default_table_expiration_ms = 3600000
}