bq_location      = "US"
dataset_id       = "yugabyte_backup"
bootstrap_bucket = "suncoast-systems-yb-bootstrap"
labels           = { env = "prod" }
secrets_project_id = "tf-k8s-cluster-infra-9734"
vault_sync_topic_name = "vault-sync-secret-events"
