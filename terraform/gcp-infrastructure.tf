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
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
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
variable "project_name"       { type = string }  # e.g. "data-pipeline"
variable "billing_account"    { type = string }  # e.g. "012345-6789AB-CDEF01"
variable "gcp_org_id"         { type = string }  # one of org_id or folder_id must be set (not both)
variable "folder_id" {
  type    = string
  default = ""                                    # e.g. "folders/123456789012"
}
variable "region"             { type = string }  # provider region (e.g. "us-central1")
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

# Eventarc region (may differ from provider region)
variable "eventarc_region" {
  type        = string
  default     = "us-east1"
  description = "Region of the Eventarc trigger/topics."
}

# Optional: exact FQN or short name of the AddSecretVersion *trigger* topic to force usage
variable "eventarc_add_version_trigger_topic_hint" {
  type        = string
  default     = "" # projects/<id>/topics/<name> OR just the topic name
  description = "If set, use this Eventarc AddSecretVersion trigger Pub/Sub topic instead of discovery."
}

# Optional: if you want to grant publish on an existing topic to a runner identity
variable "publisher_member" {
  type        = string
  default     = ""
  description = "Grant roles/pubsub.publisher on the existing Eventarc topic to this member, e.g. serviceAccount:tf-sa@proj.iam.gserviceaccount.com"
}

# Optional: free-form labels
variable "labels" {
  type    = map(string)
  default = {}
}

# ---------- Validations & locals ----------
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

  # --------- Publish debug outputs (written by local-exec) ---------
  publish_result_path = "${path.module}/.pubsub_publish_result.json"

  # --------- Exact AuditLog envelope your sync service expects ---------
  audit_method        = "google.cloud.secretmanager.v1.SecretManagerService.AddSecretVersion"
  audit_service       = "secretmanager.googleapis.com"
  # NOTE: Secret name is known at plan time; build the FQN resource for "latest"
  audit_resource_name = "projects/${var.secrets_project_id}/secrets/${google_secret_manager_secret.k8s_kafka_sa_json.secret_id}/versions/latest"
  audit_log_name      = "projects/${var.secrets_project_id}/logs/cloudaudit.googleapis.com%2Factivity"
  audit_timestamp     = timestamp()

  audit_log_entry_json = jsonencode({
    logName   = local.audit_log_name
    resource  = { type = "audited_resource" }
    timestamp = local.audit_timestamp
    protoPayload = {
      "@type"      = "type.googleapis.com/google.cloud.audit.AuditLog"
      serviceName  = local.audit_service
      methodName   = local.audit_method
      resourceName = local.audit_resource_name
    }
  })

  audit_log_entry_b64 = base64encode(local.audit_log_entry_json)
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
  keepers            = { rotated_at = timestamp() }
  depends_on         = [google_project_service.enable]
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

# Optional: grant publish on an existing Eventarc topic to a runner identity
resource "google_pubsub_topic_iam_member" "allow_publish_existing_eventarc_topic" {
  count   = var.publisher_member != "" ? 1 : 0
  project = var.secrets_project_id
  # You can supply the actual topic name via eventarc_add_version_trigger_topic_hint if you need IAM here
  topic   = var.eventarc_add_version_trigger_topic_hint != "" ? (
              startswith(var.eventarc_add_version_trigger_topic_hint, "projects/") ?
              element(split("/", var.eventarc_add_version_trigger_topic_hint), length(split("/", var.eventarc_add_version_trigger_topic_hint)) - 1)
              : var.eventarc_add_version_trigger_topic_hint
            ) : "eventarc-${var.eventarc_region}-vault-add-version-trigger-unknown"
  role    = "roles/pubsub.publisher"
  member  = var.publisher_member

  depends_on = [google_project_service.pubsub]
}

########################################
# Manual publish to existing Eventarc trigger topic when secret version changes
########################################
data "google_client_config" "cur" {}

resource "google_project_iam_audit_config" "pubsub_data_access" {
  project = var.secrets_project_id
  service = "pubsub.googleapis.com"
  audit_log_config { log_type = "DATA_READ" }
  audit_log_config { log_type = "DATA_WRITE" } # to see Publish logs
}

resource "null_resource" "notify_secret_version" {
  triggers = {
    version          = google_secret_manager_secret_version.k8s_kafka_sa_json_v.name
    secrets_project  = var.secrets_project_id
    eventarc_region  = var.eventarc_region
    secret_id        = google_secret_manager_secret.k8s_kafka_sa_json.secret_id
    topic_hint       = var.eventarc_add_version_trigger_topic_hint
  }

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    environment = {
      ACCESS_TOKEN        = data.google_client_config.cur.access_token

      # For discovery and payload
      SECRETS_PROJECT_ID  = var.secrets_project_id
      EVENTARC_REGION     = var.eventarc_region
      SECRET_ID           = google_secret_manager_secret.k8s_kafka_sa_json.secret_id
      TOPIC_HINT          = var.eventarc_add_version_trigger_topic_hint

      # Prebuilt payload from locals (ensures outputs match exactly what we published)
      AUDIT_JSON          = local.audit_log_entry_json
      AUDIT_B64           = local.audit_log_entry_b64
      PUBLISH_RESULT_PATH = local.publish_result_path
    }
    command = <<-EOT
      set -euo pipefail

      PROJECT="$SECRETS_PROJECT_ID"
      REGION="$EVENTARC_REGION"
      SECRET_ID="$SECRET_ID"
      ACCESS_TOKEN="$ACCESS_TOKEN"
      TOPIC_HINT="$TOPIC_HINT"

      echo "▶ Pub/Sub publish debug"
      echo "  project   = $PROJECT"
      echo "  region    = $REGION"
      echo "  secret_id = $SECRET_ID"

      # ---------- Resolve Eventarc *trigger* topic ----------
      if [ -n "$TOPIC_HINT" ]; then
        if [[ "$TOPIC_HINT" == projects/*/topics/* ]]; then
          TOPIC_FQN="$TOPIC_HINT"
        else
          TOPIC_FQN="projects/$PROJECT/topics/$TOPIC_HINT"
        fi
        echo "✅ Using override topic: $TOPIC_FQN"
      else
        # Need jq for discovery
        if ! command -v jq >/dev/null 2>&1; then
          echo "❌ jq is required in the runner"; exit 1
        fi

        LIST_FILE="$(mktemp)"
        LIST_CODE="$(curl -sS -o "$LIST_FILE" -w '%%{http_code}' \
          -H "Authorization: Bearer $ACCESS_TOKEN" \
          "https://pubsub.googleapis.com/v1/projects/$PROJECT/topics?pageSize=1000")"

        if [ "$LIST_CODE" -lt 200 ] || [ "$LIST_CODE" -ge 300 ]; then
          echo "❌ Pub/Sub list topics failed ($LIST_CODE): $(head -c 1000 "$LIST_FILE")"
          rm -f "$LIST_FILE"
          exit 1
        fi

        echo "  discovered topics (first few):"
        jq -r '(.topics // []) | .[].name' "$LIST_FILE" | head -n 8 | sed 's/^/    - /'

        # Prefer names that contain: eventarc-<region>- … add-version … trigger-
        TOPIC_FQN="$(jq -r --arg region "$REGION" '
          (.topics // []) | .[].name
          | select(contains("eventarc-" + $region + "-") and contains("add-version") and contains("trigger-"))
        ' "$LIST_FILE" | head -n1 || true)"

        # Fallback to the non-trigger variant (… add-version … -topic)
        if [ -z "$TOPIC_FQN" ]; then
          TOPIC_FQN="$(jq -r --arg region "$REGION" '
            (.topics // []) | .[].name
            | select(contains("eventarc-" + $region + "-") and contains("add-version") and contains("-topic"))
          ' "$LIST_FILE" | head -n1 || true)"
        fi

        rm -f "$LIST_FILE"

        if [ -z "$TOPIC_FQN" ]; then
          echo "❌ Could not find *any* Eventarc AddSecretVersion topic in region $REGION."
          echo "   Tip: set var.eventarc_add_version_trigger_topic_hint to the exact topic FQN."
          exit 1
        fi

        echo "✅ Using topic: $TOPIC_FQN"
      fi

      # ---------- Publish the prebuilt AuditLog entry ----------
      echo "  payload_len = $(printf '%s' "$AUDIT_JSON" | wc -c | tr -d ' ') bytes"

      PUB_BODY="$(jq -nc --arg d "$AUDIT_B64" '{messages:[{data:$d}]}' )"

      RESP_FILE="$(mktemp)"
      HTTP_CODE="$(curl -sS -o "$RESP_FILE" -w '%%{http_code}' \
        -H "Authorization: Bearer $ACCESS_TOKEN" \
        -H "Content-Type: application/json" \
        "https://pubsub.googleapis.com/v1/$TOPIC_FQN:publish" \
        -d "$PUB_BODY")"

      RESP_PREVIEW="$(head -c 1000 "$RESP_FILE" || true)"
      echo "  publish_http_code = $HTTP_CODE"
      echo "  publish_response  = $RESP_PREVIEW"
      rm -f "$RESP_FILE"

      # Write a JSON report we can expose via terraform outputs
      jq -nc \
        --arg project "$PROJECT" \
        --arg region "$REGION" \
        --arg secret_id "$SECRET_ID" \
        --arg topic "$TOPIC_FQN" \
        --arg http_code "$HTTP_CODE" \
        --arg resp_preview "$RESP_PREVIEW" \
        --argjson payload "$AUDIT_JSON" \
        '{project:$project, region:$region, secret_id:$secret_id, topic_used:$topic, publish_http_code:($http_code|tonumber), publish_response_preview:$resp_preview, payload_json:$payload}' \
        > "$PUBLISH_RESULT_PATH"

      if [ "$HTTP_CODE" -lt 200 ] || [ "$HTTP_CODE" -ge 300 ]; then
        echo "❌ Publish failed (HTTP $HTTP_CODE)"; exit 1
      fi

      echo "✅ Published manual sync event to Pub/Sub."
    EOT
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

# -------- Debug outputs for the Pub/Sub publish --------
output "pubsub_published_payload" {
  value       = local.audit_log_entry_json
  description = "AuditLog JSON that was published to Pub/Sub (what the sync service expects)."
}

output "pubsub_publish_result" {
  value       = try(jsondecode(file(local.publish_result_path)), null)
  description = "Publish debug report: topic used, HTTP code, short response, and payload."
}

output "pubsub_topic_used" {
  value       = try(jsondecode(file(local.publish_result_path)).topic_used, null)
  description = "Fully-qualified topic that received the message."
}

output "pubsub_publish_http_code" {
  value       = try(jsondecode(file(local.publish_result_path)).publish_http_code, null)
  description = "HTTP status returned by Pub/Sub publish."
}

output "pubsub_publish_response_preview" {
  value       = try(jsondecode(file(local.publish_result_path)).publish_response_preview, null)
  description = "First ~1000 bytes of the Pub/Sub publish response (for quick debugging)."
}
