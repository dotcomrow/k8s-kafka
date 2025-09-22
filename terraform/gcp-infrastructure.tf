########################################
# Providers & variables
########################################
terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.40"
    }
  }
}

# Create random suffix
resource "random_id" "suffix_gcp" {
  byte_length = 2
}

# This provider is used both to create the project and, later, to operate inside it.
# We still set 'project' so resources that don't accept a 'project' field will default correctly.
provider "google" {
  project = var.project_id
  region  = var.region
}

# ---------- Inputs ----------
variable "project_name" { type = string }                  # e.g. "Data Pipeline"
variable "project_id"   { type = string }                  # desired project ID (must be globally unique)
variable "billing_account" { type = string }               # e.g. "012345-6789AB-CDEF01"
variable "gcp_org_id" {
  type    = string
}     # one of org_id or folder_id must be set (not both)
variable "folder_id" {
  type    = string
}     # e.g. "folders/123456789012"
variable "region"      { type = string }                   # e.g. "us-central1" (for provider)
variable "bq_location" { type = string }                   # e.g. "US" or "EU"
variable "dataset_id"  { type = string }                   # e.g. "analytics"
variable "bootstrap_bucket" { type = string }              # globally-unique bucket name
variable "sa_name" {
  type    = string
  default = "bq-data-pipeline"                             # one SA used by both jobs
}
variable "secret_id" {
  type    = string
  default = "bq-data-pipeline-key"                         # Secret Manager secret name
}

# Optional: free-form labels
variable "labels" {
  type    = map(string)
  default = {}
}

# ---------- Validations ----------
locals {
  parent_provided = (var.gcp_org_id != "" ? 1 : 0) + (var.folder_id != "" ? 1 : 0)
}

# Must set exactly one parent (org or folder)
resource "null_resource" "validate_parent" {
  lifecycle { ignore_changes = all }
  provisioner "local-exec" {
    when    = create
    command = "test ${local.parent_provided} -eq 1 || (echo 'Set exactly one of org_id or folder_id' >&2; exit 1)"
  }
}

########################################
# Project + API enablement
########################################
resource "google_project" "this" {
  name            = var.project_name
  billing_account = var.billing_account
  project_id      = "${var.project_name}-${random_id.suffix_gcp.hex}"
  # Exactly one of these must be set; use null for the other
  org_id    = var.gcp_org_id    != "" ? var.gcp_org_id    : null
  folder_id = var.folder_id != "" ? var.folder_id : null

  labels = var.labels

  depends_on = [null_resource.validate_parent]
}

# Required services in the new project
locals {
  required_services = [
    "bigquery.googleapis.com",
    "bigquerystorage.googleapis.com",
    "iam.googleapis.com",
    "secretmanager.googleapis.com",
    "storage.googleapis.com",
  ]
}

resource "google_project_service" "enable" {
  for_each           = toset(local.required_services)
  project            = google_project.this.project_id
  service            = each.key
  disable_on_destroy = false
}

########################################
# BigQuery dataset (target for sink)
########################################
resource "google_bigquery_dataset" "target" {
  project                     = google_project.this.project_id
  dataset_id                  = var.dataset_id
  location                    = var.bq_location    # must be US/EU family compatible with bucket
  delete_contents_on_destroy  = false
  labels                      = var.labels

  depends_on = [google_project_service.enable]
}

########################################
# GCS bucket for bootstrap exports
########################################
resource "google_storage_bucket" "bootstrap" {
  project                      = google_project.this.project_id
  name                         = var.bootstrap_bucket
  location                     = var.bq_location   # use US/EU multi-region to match BigQuery location family
  force_destroy                = false
  uniform_bucket_level_access  = true
  labels                       = var.labels

  lifecycle_rule {
    action { type = "Delete" }
    condition { age = 3 }      # auto-clean bootstrap files after N days
  }

  depends_on = [google_project_service.enable]
}

########################################
# Service Account + minimal IAM
########################################
resource "google_service_account" "pipeline" {
  project      = google_project.this.project_id
  account_id   = var.sa_name
  display_name = "Kafka↔BQ pipeline SA (bootstrap export + sink)"
}

# Allow BigQuery jobs in the project
resource "google_project_iam_member" "sa_bq_job_user" {
  project = google_project.this.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline.email}"

  depends_on = [google_project_service.enable]
}

# Allow writes into the target dataset (sink)
resource "google_bigquery_dataset_iam_member" "sa_dataset_editor" {
  project    = google_project.this.project_id
  dataset_id = google_bigquery_dataset.target.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"

  depends_on = [google_project_service.enable]
}

# Allow bootstrap export to write to the GCS bucket
resource "google_storage_bucket_iam_member" "sa_bucket_object_creator" {
  bucket = google_storage_bucket.bootstrap.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.pipeline.email}"

  depends_on = [google_project_service.enable]
}

########################################
# Create a key and store in Secret Manager
########################################
resource "google_service_account_key" "pipeline_key" {
  service_account_id = google_service_account.pipeline.name
  keepers = {
    # rotate by changing this value (e.g., commit timestamp or manual value)
    rotated_at = timestamp()
  }

  depends_on = [google_project_service.enable]
}

resource "google_secret_manager_secret" "pipeline_key" {
  project   = google_project.this.project_id
  secret_id = var.secret_id
  replication {
    auto {}
  }   # provider v5 syntax

  labels = var.labels

  depends_on = [google_project_service.enable]
}

resource "google_secret_manager_secret_version" "pipeline_key_v" {
  secret      = google_secret_manager_secret.pipeline_key.id
  # SA key is base64; Secret Manager expects raw JSON
  secret_data = base64decode(google_service_account_key.pipeline_key.private_key)
}

# ------------------------------------------------------------
# Save SA key JSON to Secret Manager so it can sync to Vault
# Name must be EXACT (user requested):
#   k8s-kafka-gcp-sesrvice-account-json
# ------------------------------------------------------------

# Secret container (auto replication; provider v5 syntax)
resource "google_secret_manager_secret" "k8s_kafka_sa_json" {
  project   = google_project.this.project_id
  secret_id = "k8s-kafka-gcp-sesrvice-account-json"

  replication {
    auto {}
  }   # provider v5 syntax

  # (optional) labels
  # labels = var.labels

  depends_on = [google_project_service.enable]
}

# Put the SA key JSON into the secret (latest version)
# NOTE: google_service_account_key.pipeline_key.private_key is base64; decode to raw JSON
resource "google_secret_manager_secret_version" "k8s_kafka_sa_json_v" {
  secret      = google_secret_manager_secret.k8s_kafka_sa_json.id
  secret_data = base64decode(google_service_account_key.pipeline_key.private_key)
}

########################################
# Outputs (wire these into your Vault config job)
########################################
output "project_id" {
  value       = google_project.this.project_id
  description = "Newly created GCP project ID"
}

output "bq_project" {
  value       = google_project.this.project_id
  description = "Project hosting BigQuery resources"
}

output "bq_dataset" {
  value = google_bigquery_dataset.target.dataset_id
}

output "bq_location" {
  value = var.bq_location
}

output "bootstrap_bucket" {
  value = google_storage_bucket.bootstrap.name
}

output "pipeline_sa_email" {
  value = google_service_account.pipeline.email
}

output "pipeline_sa_key_secret_name" {
  value       = google_secret_manager_secret.pipeline_key.name
  description = "projects/<num>/secrets/<name>; read latest version for key JSON"
}
