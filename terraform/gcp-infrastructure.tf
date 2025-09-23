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

# Optional: Eventarc region hint used when auto-discovering trigger topic (e.g. us-east1)
variable "eventarc_region" {
  type        = string
  description = "Eventarc region used to prefer a matching trigger topic during auto-discovery"
  default     = ""
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

resource "null_resource" "notify_secret_version" {
  triggers = {
    version        = google_secret_manager_secret_version.k8s_kafka_sa_json_v.name
    secrets_proj   = var.secrets_project_id
    secret_id      = google_secret_manager_secret.k8s_kafka_sa_json.secret_id
    # include user-provided topic + region in triggers so we re-run if they change
    trigger_topic  = var.vault_eventarc_trigger_topic_name
    eventarc_reg   = var.eventarc_region
  }

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    command = <<-EOT
      set -euo pipefail

      PROJECT="$SECRETS_PROJECT_ID"
      SECRET_ID="$SECRET_ID"
      ACCESS_TOKEN="$ACCESS_TOKEN"

      # Inputs for topic selection
      PINNED_TOPIC="$EVENTARC_TRIGGER_TOPIC_NAME"   # may be empty
      REGION_HINT="$EVENTARC_REGION"                # may be empty

      pick_topic() {
        # If caller pinned a topic, use it verbatim (and verify it exists)
        if [ -n "$PINNED_TOPIC" ]; then
          local code
          code="$(curl -sS -o /dev/null -w '%%{http_code}' \
                 -H "Authorization: Bearer $ACCESS_TOKEN" \
                 "https://pubsub.googleapis.com/v1/projects/$PROJECT/topics/$PINNED_TOPIC")"
          if [ "$code" = "200" ]; then
            echo "$PINNED_TOPIC"
            return 0
          fi
          echo "❌ Pinned topic not found: projects/$PROJECT/topics/$PINNED_TOPIC (HTTP $code)" >&2
          return 1
        fi

        # Auto-discover: list topics and pick an Eventarc trigger topic
        # Pattern: eventarc-<region>-vault-add-version-trigger-<suffix>
        local list
        list="$(curl -sS \
          -H "Authorization: Bearer $ACCESS_TOKEN" \
          "https://pubsub.googleapis.com/v1/projects/$PROJECT/topics")"

        # Pull names, strip full resource path
        local names
        names="$(echo "$list" | jq -r '.topics[].name' 2>/dev/null | sed 's#.*/topics/##' || true)"

        # Filter to trigger topics for AddSecretVersion
        local filtered
        filtered="$(echo "$names" | grep -E '^eventarc-.*-vault-add-version-trigger-[0-9]+' || true)"

        # If region hint is set, prefer those; otherwise keep as-is
        if [ -n "$REGION_HINT" ]; then
          local regional
          regional="$(echo "$filtered" | grep -E "^eventarc-$REGION_HINT-.*-vault-add-version-trigger-[0-9]+" || true)"
          if [ -n "$regional" ]; then
            filtered="$regional"
          fi
        fi

        # If exactly one match, use it; if none or many, print guidance
        local count
        count="$(printf '%s\n' "$filtered" | sed '/^$/d' | wc -l | tr -d ' ')"
        if [ "$count" = "1" ]; then
          echo "$filtered"
          return 0
        fi

        echo "❌ Could not uniquely determine Eventarc trigger topic." >&2
        echo "   Found ($count) candidates matching '*-vault-add-version-trigger-*'" >&2
        if [ "$count" != "0" ]; then
          echo "$filtered" >&2
        fi
        echo "   Set var.vault_eventarc_trigger_topic_name to the exact topic name," >&2
        echo "   or set var.eventarc_region to prefer a specific region (e.g. us-east1)." >&2
        return 1
      }

      TOPIC="$(pick_topic)"

      echo "▶ Pub/Sub publish debug"
      echo "  project   = $PROJECT"
      echo "  topic     = $TOPIC"
      echo "  secret_id = $SECRET_ID"

      # Build audit-style payload your sync service expects
      RNAME="projects/$PROJECT/secrets/$SECRET_ID/versions/latest"
      PAYLOAD_JSON="$(jq -nc \
        --arg method 'google.cloud.secretmanager.v1.SecretManagerService.AddSecretVersion' \
        --arg rname  "$RNAME" \
        '{protoPayload:{serviceName:"secretmanager.googleapis.com", methodName:$method, resourceName:$rname}}')"

      # Base64 encode WITHOUT newlines
      BASE64_PAYLOAD="$(printf '%s' "$PAYLOAD_JSON" | base64 2>/dev/null | tr -d '\n\r')"

      # Assemble publish body
      PUB_BODY="$(jq -nc --arg d "$BASE64_PAYLOAD" '{messages:[{data:$d}]}' )"

      RESP_FILE="$(mktemp)"
      HTTP_CODE="$(curl -sS -o "$RESP_FILE" -w '%%{http_code}' \
        -H "Authorization: Bearer $ACCESS_TOKEN" \
        -H "Content-Type: application/json" \
        "https://pubsub.googleapis.com/v1/projects/$PROJECT/topics/$TOPIC:publish" \
        -d "$PUB_BODY")"

      echo "  publish_http_code = $HTTP_CODE"
      echo "  publish_response  = $(head -c 1000 "$RESP_FILE")"
      rm -f "$RESP_FILE"

      [ "$HTTP_CODE" -ge 200 ] && [ "$HTTP_CODE" -lt 300 ] || { echo "❌ Publish failed"; exit 1; }

      echo "✅ Published manual sync event to Pub/Sub."
    EOT

    environment = {
      ACCESS_TOKEN                = data.google_client_config.cur.access_token
      SECRETS_PROJECT_ID          = var.secrets_project_id
      SECRET_ID                   = google_secret_manager_secret.k8s_kafka_sa_json.secret_id
      EVENTARC_TRIGGER_TOPIC_NAME = var.vault_eventarc_trigger_topic_name
      EVENTARC_REGION             = var.eventarc_region
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
