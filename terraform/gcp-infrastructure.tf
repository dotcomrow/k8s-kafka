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
provider "google" {
  project = local.project_id
  region  = var.region
}

# ---------- Inputs ----------
variable "secrets_project_id" { type = string }  # e.g. "tf-k8s-cluster-infra-9734"
variable "project_name"       { type = string }  # e.g. "Data Pipeline"
variable "billing_account"    { type = string }  # e.g. "012345-6789AB-CDEF01"
variable "gcp_org_id"         { type = string }  # one of org_id or folder_id must be set (not both)
variable "folder_id" {
  type    = string
  default = ""
} # e.g. "folders/123456789012"
variable "region"             { type = string }  # e.g. "us-central1" (for provider)
variable "bq_location"        { type = string }  # e.g. "US" or "EU"
variable "dataset_id"         { type = string }  # e.g. "analytics"
variable "bootstrap_bucket"   { type = string }  # globally-unique bucket name
variable "sa_name" {
  type    = string
  default = "bq-data-pipeline"
}
variable "secret_id" {
  type    = string
  default = "bq-data-pipeline-key"
}
# Optional: exact name of the Eventarc trigger Pub/Sub topic to publish to
variable "vault_eventarc_trigger_topic_name" {
  type        = string
  description = "Existing Eventarc trigger Pub/Sub topic name (e.g. eventarc-us-east1-vault-add-version-trigger-287)"
  default     = ""
}

variable "eventarc_add_version_topic_override" {
  type        = string
  default     = "" # e.g. "projects/tf-k8s-cluster-infra-9734/topics/eventarc-us-east1-vault-add-version-trigger-287"
  description = "If set, use this Pub/Sub topic (full resource or short name) instead of discovery."
}

# Eventarc topic usage (existing topics)
variable "vault_eventarc_topic_name" {
  type        = string
  default     = ""
  description = "Existing Eventarc Pub/Sub topic name (no projects/... prefix). If empty, will fall back to eventarc-<eventarc_region>-vault-add-version-topic."
}

variable "eventarc_region" {
  type        = string
  default     = "us-east1"
  description = "Region of the Eventarc trigger/topics."
}

# Optional: grant publish to an identity on the existing topic
variable "publisher_member" {
  type        = string
  default     = ""
  description = "Optional IAM member to grant roles/pubsub.publisher on the existing Eventarc topic, e.g. serviceAccount:tf-sa@proj.iam.gserviceaccount.com"
}

# Optional: free-form labels
variable "labels" {
  type    = map(string)
  default = {}
}

# ---------- Validations ----------
locals {
  parent_provided = (var.gcp_org_id != "" ? 1 : 0) + (var.folder_id != "" ? 1 : 0)
  project_id      = "${var.project_name}-${random_id.suffix_gcp.hex}"

  kafka_apis = toset([
    "bigquery.googleapis.com",
    "bigquerystorage.googleapis.com",
    "iam.googleapis.com",
    "logging.googleapis.com",
    "secretmanager.googleapis.com",
    "storage.googleapis.com",
  ])
}

# Ensure Pub/Sub API is enabled in the secrets/infra project (where Eventarc topics live)
resource "google_project_service" "pubsub" {
  project = var.secrets_project_id
  service = "pubsub.googleapis.com"
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
# Project + API enablement (Kafka project)
########################################
resource "google_project" "this" {
  name            = var.project_name
  billing_account = var.billing_account
  project_id      = local.project_id
  org_id          = var.gcp_org_id != "" ? var.gcp_org_id : null
  folder_id       = var.folder_id   != "" ? var.folder_id : null
  labels          = var.labels

  depends_on = [null_resource.validate_parent]
}

resource "google_project_service" "enable" {
  for_each           = local.kafka_apis
  project            = local.project_id
  service            = each.value
  disable_on_destroy = false
}

########################################
# BigQuery dataset (target for sink)
########################################
resource "google_bigquery_dataset" "target" {
  project                    = google_project.this.project_id
  dataset_id                 = var.dataset_id
  location                   = var.bq_location
  delete_contents_on_destroy = false
  labels                     = var.labels
  depends_on                 = [google_project_service.enable]
}

########################################
# GCS bucket for bootstrap exports
########################################
resource "google_storage_bucket" "bootstrap" {
  project                      = google_project.this.project_id
  name                         = var.bootstrap_bucket
  location                     = var.bq_location
  force_destroy                = false
  uniform_bucket_level_access  = true
  labels                       = var.labels

  lifecycle_rule {
    action    { type = "Delete" }
    condition { age  = 3 }
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

resource "google_project_iam_member" "sa_bq_job_user" {
  project = google_project.this.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
  depends_on = [google_project_service.enable]
}

resource "google_bigquery_dataset_iam_member" "sa_dataset_editor" {
  project    = google_project.this.project_id
  dataset_id = google_bigquery_dataset.target.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
  depends_on = [google_project_service.enable]
}

resource "google_storage_bucket_iam_member" "sa_bucket_object_creator" {
  bucket = google_storage_bucket.bootstrap.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.pipeline.email}"
  depends_on = [google_project_service.enable]
}

########################################
# Create a key and store in Secret Manager (Kafka project)
########################################
resource "google_service_account_key" "pipeline_key" {
  service_account_id = google_service_account.pipeline.name
  keepers = { rotated_at = timestamp() }
  depends_on = [google_project_service.enable]
}

resource "google_secret_manager_secret" "pipeline_key" {
  project   = google_project.this.project_id
  secret_id = var.secret_id
  replication {
    auto {}
  }   # provider v5 syntax
  labels     = var.labels
  depends_on = [google_project_service.enable]
}

resource "google_secret_manager_secret_version" "pipeline_key_v" {
  secret      = google_secret_manager_secret.pipeline_key.id
  secret_data = base64decode(google_service_account_key.pipeline_key.private_key)
}

# ------------------------------------------------------------
# Save SA key JSON to Secret Manager in the infra/secrets project
# ------------------------------------------------------------
resource "google_secret_manager_secret" "k8s_kafka_sa_json" {
  project   = var.secrets_project_id
  secret_id = "k8s-kafka-gcp-service-account-json"
  replication {
    auto {}
  }   # provider v5 syntax
  depends_on = [google_project_service.pubsub] # ensure API on the target project
}

resource "google_secret_manager_secret_version" "k8s_kafka_sa_json_v" {
  secret      = google_secret_manager_secret.k8s_kafka_sa_json.id
  secret_data = base64decode(google_service_account_key.pipeline_key.private_key)
}

# Optional: grant publish on existing Eventarc topic to a runner identity
resource "google_pubsub_topic_iam_member" "allow_publish_existing_eventarc_topic" {
  count   = var.vault_eventarc_topic_name != "" && var.publisher_member != "" ? 1 : 0
  project = var.secrets_project_id
  topic   = var.vault_eventarc_topic_name
  role    = "roles/pubsub.publisher"
  member  = var.publisher_member

  depends_on = [google_project_service.pubsub]
}

########################################
# Manual publish to existing Eventarc topic when secret version changes
########################################
data "google_client_config" "cur" {}

resource "google_project_iam_audit_config" "pubsub_data_access" {
  project = var.secrets_project_id
  service = "pubsub.googleapis.com"
  audit_log_config { log_type = "DATA_READ" }
  audit_log_config { log_type = "DATA_WRITE" } # to see Publish logs
}

# ---------- inputs ----------
variable "vault_sync_topic_name" { type = string } # e.g. "eventarc-us-east1-vault-add-version-topic"

resource "null_resource" "notify_secret_version" {
  triggers = {
    version      = google_secret_manager_secret_version.k8s_kafka_sa_json_v.name
    secrets_proj = var.secrets_project_id
    region       = var.region
    secret_id    = google_secret_manager_secret.k8s_kafka_sa_json.secret_id
  }

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    command = <<-EOT
      set -euo pipefail

      PROJECT="$${SECRETS_PROJECT_ID}"
      REGION="$${REGION}"
      SECRET_ID="$${SECRET_ID}"
      ACCESS_TOKEN="$${ACCESS_TOKEN}"

      echo "▶ Pub/Sub publish debug"
      echo "  project   = $PROJECT"
      echo "  region    = $REGION"
      echo "  secret_id = $SECRET_ID"

      # ===== Discover the EXACT Eventarc trigger topic via Eventarc API =====
      METHOD="google.cloud.secretmanager.v1.SecretManagerService.AddSecretVersion"
      SERVICE="secretmanager.googleapis.com"

      TRIGGERS_JSON="$(curl -sS -H "Authorization: Bearer $ACCESS_TOKEN" \
        "https://eventarc.googleapis.com/v1/projects/$PROJECT/locations/$REGION/triggers")"

      TOPIC_FQN="$(printf '%s' "$TRIGGERS_JSON" \
        | jq -r --arg m "$METHOD" --arg s "$SERVICE" '
            .triggers[]? as $t
            | select( ([$t.matchingCriteria[]? | select(.attribute=="serviceName") | .value] | index($s)) != null )
            | select( ([$t.matchingCriteria[]? | select(.attribute=="methodName") | .value] | index($m)) != null )
            | .transport.pubsub.topic // empty
          ' \
        | head -n1 || true)"

      if [ -z "$TOPIC_FQN" ]; then
        echo "❌ Could not find an Eventarc trigger topic for AddSecretVersion in $REGION."; exit 1
      fi
      echo "✅ Using trigger topic: $TOPIC_FQN"

      # ===== Build payload expected by sync service (AuditLog-like) =====
      PAYLOAD_JSON="$(jq -nc \
        --arg svc   "$SERVICE" \
        --arg method "$METHOD" \
        --arg rname "projects/$PROJECT/secrets/$SECRET_ID/versions/latest" \
        '{protoPayload:{serviceName:$svc, methodName:$method, resourceName:$rname}}')"
      echo "  payload_len = $(printf '%s' "$PAYLOAD_JSON" | wc -c | tr -d ' ') bytes"

      BASE64_PAYLOAD="$(printf '%s' "$PAYLOAD_JSON" | base64 | tr -d '\n')"
      PUB_BODY="$(jq -nc --arg d "$BASE64_PAYLOAD" '{messages:[{data:$d}]}' )"

      # ===== Publish to the trigger topic =====
      RESP_FILE="$(mktemp)"
      HTTP_CODE="$(curl -sS -o "$RESP_FILE" -w '%%{http_code}' \
        -H "Authorization: Bearer $ACCESS_TOKEN" \
        -H "Content-Type: application/json" \
        "https://pubsub.googleapis.com/v1/$TOPIC_FQN:publish" \
        -d "$PUB_BODY")"

      echo "  publish_http_code = $HTTP_CODE"
      echo "  publish_response  = $(head -c 1000 "$RESP_FILE")"
      rm -f "$RESP_FILE"

      if [ "$HTTP_CODE" -lt 200 ] || [ "$HTTP_CODE" -ge 300 ]; then
        echo "❌ Publish failed (HTTP $HTTP_CODE)"; exit 1
      fi

      echo "✅ Published manual sync event to Pub/Sub."
    EOT

    environment = {
      ACCESS_TOKEN       = data.google_client_config.cur.access_token
      SECRETS_PROJECT_ID = var.secrets_project_id
      REGION             = var.region
      SECRET_ID          = google_secret_manager_secret.k8s_kafka_sa_json.secret_id
    }
  }
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
  description = "projects/<num>/secrets/<name>; read latest secret version for key JSON"
}
