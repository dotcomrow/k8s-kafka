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
    external = {
      source  = "hashicorp/external"
      version = "~> 2.3"
    }
  }
}

resource "random_id" "suffix_gcp" {
  byte_length = 2
}

provider "google" {
  project = local.project_id
  region  = var.region
}

# ---------- Inputs ----------
variable "secrets_project_id" { type = string }
variable "project_name"       { type = string }
variable "billing_account"    { type = string }
variable "gcp_org_id"         { type = string }
variable "folder_id" {
  type    = string
  default = ""
}
variable "region"             { type = string }
variable "bq_location"        { type = string }
variable "dataset_id"         { type = string }
variable "bootstrap_bucket"   { type = string }
variable "sa_name" {
  type    = string
  default = "bq-data-pipeline"
}
variable "secret_id" {
  type    = string
  default = "bq-data-pipeline-key"
}

# Eventarc region (where triggers live)
variable "eventarc_region" {
  type        = string
  default     = "us-east1"
  description = "Region of the Eventarc trigger/topics."
}

# Optional: force the exact AddSecretVersion *trigger* topic (FQN or short name)
variable "eventarc_add_version_trigger_topic_hint" {
  type        = string
  default     = ""
  description = "projects/<id>/topics/<name> OR just the topic name"
}

# Optional IAM grant
variable "publisher_member" {
  type        = string
  default     = ""
  description = "Grant roles/pubsub.publisher on the existing Eventarc topic to this member"
}

# Optional labels
variable "labels" {
  type    = map(string)
  default = {}
}

# ---------- Locals ----------
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

  # Prebuilt AuditLog payload (what your sync service expects)
  audit_method        = "google.cloud.secretmanager.v1.SecretManagerService.AddSecretVersion"
  audit_service       = "secretmanager.googleapis.com"
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

  audit_log_entry_b64  = base64encode(local.audit_log_entry_json)
  publish_result_path  = "${path.module}/.pubsub_publish_result.json"

  # If the hint is provided, compute an "effective" FQN (used if discovery is skipped)
  eventarc_effective_trigger_topic_fqn = var.eventarc_add_version_trigger_topic_hint != "" ? (
    startswith(var.eventarc_add_version_trigger_topic_hint, "projects/") ?
    var.eventarc_add_version_trigger_topic_hint :
    "projects/${var.secrets_project_id}/topics/${var.eventarc_add_version_trigger_topic_hint}"
  ) : ""
}

# Ensure Pub/Sub API in the secrets/infra project
resource "google_project_service" "pubsub" {
  project = var.secrets_project_id
  service = "pubsub.googleapis.com"
}

# Validate exactly one parent
resource "null_resource" "validate_parent" {
  lifecycle { ignore_changes = all }
  provisioner "local-exec" {
    when    = create
    command = "test ${local.parent_provided} -eq 1 || (echo 'Set exactly one of org_id or folder_id' >&2; exit 1)"
  }
}

########################################
# Kafka project + APIs
########################################
resource "google_project" "this" {
  name            = var.project_name
  billing_account = var.billing_account
  project_id      = local.project_id
  org_id          = var.gcp_org_id != "" ? var.gcp_org_id : null
  folder_id       = var.folder_id   != "" ? var.folder_id : null
  labels          = var.labels
  depends_on      = [null_resource.validate_parent]
}

resource "google_project_service" "enable" {
  for_each           = local.kafka_apis
  project            = local.project_id
  service            = each.value
  disable_on_destroy = false
}

########################################
# BigQuery + GCS
########################################
resource "google_bigquery_dataset" "target" {
  project                    = google_project.this.project_id
  dataset_id                 = var.dataset_id
  location                   = var.bq_location
  delete_contents_on_destroy = false
  labels                     = var.labels
  depends_on                 = [google_project_service.enable]
}

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
# Service Account + IAM
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
# SA key → Secret Manager (Kafka project)
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

########################################
# SA key → Secret Manager (secrets/infra project)
########################################
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

# Optional IAM on existing topic
resource "google_pubsub_topic_iam_member" "allow_publish_existing_eventarc_topic" {
  count   = var.publisher_member != "" && var.eventarc_add_version_trigger_topic_hint != "" ? 1 : 0
  project = var.secrets_project_id
  topic   = startswith(var.eventarc_add_version_trigger_topic_hint, "projects/") ? element(split("/", var.eventarc_add_version_trigger_topic_hint), length(split("/", var.eventarc_add_version_trigger_topic_hint)) - 1) : var.eventarc_add_version_trigger_topic_hint
  role    = "roles/pubsub.publisher"
  member  = var.publisher_member
  depends_on = [google_project_service.pubsub]
}

########################################
# Discover Eventarc trigger topic in Terraform (so we can output it)
########################################
data "google_client_config" "cur" {}

# Uses curl + jq to list topics and choose the *trigger* one
data "external" "discover_add_version_trigger_topic" {
  program = ["/bin/bash", "-c", <<-EOS
    set -euo pipefail
    # read the JSON query from stdin
    QUERY="$(cat)"
    PROJECT="$(printf '%s' "$QUERY" | jq -r '.project')"
    REGION="$(printf '%s' "$QUERY" | jq -r '.region')"
    TOKEN="$(printf '%s' "$QUERY" | jq -r '.token')"
    HINT="$(printf '%s' "$QUERY" | jq -r '.hint')"

    if [ -n "$HINT" ] && [ "$HINT" != "null" ]; then
      if [[ "$HINT" == projects/*/topics/* ]]; then
        printf '{"topic_fqn":"%s"}' "$HINT"
      else
        printf '{"topic_fqn":"projects/%s/topics/%s"}' "$PROJECT" "$HINT"
      fi
      exit 0
    fi

    # Discover via Pub/Sub REST
    OUT="$(curl -sS -H "Authorization: Bearer $TOKEN" "https://pubsub.googleapis.com/v1/projects/$PROJECT/topics?pageSize=1000")"

    TOPIC="$(printf '%s' "$OUT" | jq -r --arg region "$REGION" '(.topics // []) | .[].name | select(contains("eventarc-" + $region + "-") and contains("add-version") and contains("trigger-"))' | head -n1)"
    if [ -z "$TOPIC" ]; then
      TOPIC="$(printf '%s' "$OUT" | jq -r --arg region "$REGION" '(.topics // []) | .[].name | select(contains("eventarc-" + $region + "-") and contains("add-version") and contains("-topic"))' | head -n1)"
    fi

    if [ -z "$TOPIC" ]; then
      printf '{"topic_fqn":""}'
    else
      jq -n --arg t "$TOPIC" '{"topic_fqn":$t}'
    fi
  EOS
  ]

  query = {
    project = var.secrets_project_id
    region  = var.eventarc_region
    token   = data.google_client_config.cur.access_token
    hint    = var.eventarc_add_version_trigger_topic_hint
  }
}

########################################
# Manual publish to Eventarc trigger topic
########################################
resource "google_project_iam_audit_config" "pubsub_data_access" {
  project = var.secrets_project_id
  service = "pubsub.googleapis.com"
  audit_log_config { log_type = "DATA_READ" }
  audit_log_config { log_type = "DATA_WRITE" }
}

resource "null_resource" "notify_secret_version" {
  triggers = {
    version         = google_secret_manager_secret_version.k8s_kafka_sa_json_v.name
    secrets_project = var.secrets_project_id
    eventarc_region = var.eventarc_region
    secret_id       = google_secret_manager_secret.k8s_kafka_sa_json.secret_id
    topic_hint      = var.eventarc_add_version_trigger_topic_hint
    discovered      = try(data.external.discover_add_version_trigger_topic.result["topic_fqn"], "")
  }

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    environment = {
      ACCESS_TOKEN        = data.google_client_config.cur.access_token
      SECRETS_PROJECT_ID  = var.secrets_project_id
      EVENTARC_REGION     = var.eventarc_region
      SECRET_ID           = google_secret_manager_secret.k8s_kafka_sa_json.secret_id
      # prefer discovered FQN; may be ""
      TOPIC_FQN           = try(data.external.discover_add_version_trigger_topic.result["topic_fqn"], "")
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
      TOPIC_FQN="$TOPIC_FQN"

      echo "▶ Pub/Sub publish debug"
      echo "  project   = $PROJECT"
      echo "  region    = $REGION"
      echo "  secret_id = $SECRET_ID"

      # If discovery didn't find anything (or you're missing jq on the runner), bail with a helpful hint
      if [ -z "$TOPIC_FQN" ]; then
        echo "❌ No trigger topic discovered. Set var.eventarc_add_version_trigger_topic_hint (FQN or name)."
        exit 1
      fi
      echo "✅ Using topic: $TOPIC_FQN"

      # ---------- CloudEvents attributes ----------
      CE_TYPE="google.cloud.audit.log.v1.written"
      CE_SOURCE="//cloudaudit.googleapis.com/projects/$PROJECT/logs/cloudaudit.googleapis.com%2Factivity"
      CE_SPEC="1.0"
      CE_TIME="$(date -u +%FT%TZ)"
      if command -v uuidgen >/dev/null 2>&1; then CE_ID="$(uuidgen)"; else CE_ID="$(date +%s%N)-manual"; fi

      echo "  payload_len = $(printf '%s' "$AUDIT_JSON" | wc -c | tr -d ' ') bytes"

      PUB_BODY="$(jq -nc --arg d "$AUDIT_B64" \
        --arg t "$CE_TYPE" --arg s "$CE_SOURCE" --arg v "$CE_SPEC" --arg i "$CE_ID" --arg tm "$CE_TIME" \
        '{messages:[{data:$d, attributes:{ "ce-type":$t, "ce-source":$s, "ce-specversion":$v, "ce-id":$i, "ce-time":$tm, "content-type":"application/json"}}]}')"

      RESP_FILE="$(mktemp)"
      HTTP_CODE="$(curl -sS -o "$RESP_FILE" -w '%%{http_code}' \
        -H "Authorization: Bearer $ACCESS_TOKEN" \
        -H "Content-Type: application/json" \
        "https://pubsub.googleapis.com/v1/$TOPIC_FQN:publish" \
        -d "$PUB_BODY")"

      RESP_PREVIEW="$(head -c 1000 "$RESP_FILE" || true)"
      MESSAGE_IDS="$(jq -r '.messageIds // empty | join(",")' "$RESP_FILE" 2>/dev/null || true)"
      rm -f "$RESP_FILE"

      echo "  publish_http_code = $HTTP_CODE"
      echo "  message_ids       = $MESSAGE_IDS"
      echo "  publish_response  = $RESP_PREVIEW"

      PAYLOAD_PREVIEW="$(printf '%s' "$AUDIT_JSON" | head -c 500)"
      jq -nc \
        --arg project "$PROJECT" \
        --arg region "$REGION" \
        --arg secret_id "$SECRET_ID" \
        --arg topic "$TOPIC_FQN" \
        --arg http_code "$HTTP_CODE" \
        --arg mids "$MESSAGE_IDS" \
        --arg payload_preview "$PAYLOAD_PREVIEW" \
        --argjson payload "$AUDIT_JSON" \
        '{
          project: $project,
          region: $region,
          secret_id: $secret_id,
          topic_used: $topic,
          publish_http_code: ($http_code | tonumber),
          message_ids: (if $mids == "" then [] else ($mids | split(",")) end),
          payload_json: $payload,
          payload_preview: $payload_preview
        }' > "$PUBLISH_RESULT_PATH"

      if [ "$HTTP_CODE" -lt 200 ] || [ "$HTTP_CODE" -ge 300 ]; then
        echo "❌ Publish failed (HTTP $HTTP_CODE)"; exit 1
      fi

      echo "✅ Published manual sync event to Pub/Sub."
    EOT
  }
}

########################################
# Outputs
########################################
output "project_id"                   { value = google_project.this.project_id }
output "bq_project"                   { value = google_project.this.project_id }
output "bq_dataset"                   { value = google_bigquery_dataset.target.dataset_id }
output "bq_location"                  { value = var.bq_location }
output "bootstrap_bucket"             { value = google_storage_bucket.bootstrap.name }
output "pipeline_sa_email"            { value = google_service_account.pipeline.email }
output "pipeline_sa_key_secret_name"  { value = google_secret_manager_secret.pipeline_key.name }

# Prebuilt payload (exact JSON your sync service expects)
output "pubsub_published_payload" {
  value       = local.audit_log_entry_json
  description = "AuditLog JSON that was published to Pub/Sub."
}

# The discovered (or hinted) Eventarc trigger topic FQN — purely Terraform-level (no file needed)
output "eventarc_trigger_topic_fqn" {
  value       = coalesce(
                  try(data.external.discover_add_version_trigger_topic.result["topic_fqn"], ""),
                  local.eventarc_effective_trigger_topic_fqn != "" ? local.eventarc_effective_trigger_topic_fqn : null
               )
  description = "Eventarc AddSecretVersion trigger topic FQN used for publishing."
}

# Best-effort file-based report (from the publisher script)
output "pubsub_publish_result" {
  value       = try(jsondecode(file(local.publish_result_path)), null)
  description = "Publish report: topic used, message IDs, HTTP code, payload preview."
}

output "pubsub_topic_used" {
  value       = try(jsondecode(file(local.publish_result_path)).topic_used, null)
  description = "Fully-qualified topic that received the message (best-effort, from runtime)."
}

output "pubsub_publish_http_code" {
  value       = try(jsondecode(file(local.publish_result_path)).publish_http_code, null)
  description = "HTTP status from Pub/Sub publish (best-effort)."
}

output "pubsub_publish_message_ids" {
  value       = try(jsondecode(file(local.publish_result_path)).message_ids, null)
  description = "Pub/Sub message IDs from the publish (best-effort)."
}
