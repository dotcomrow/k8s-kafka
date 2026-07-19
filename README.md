# k8s-kafka
Kafka system and components for Suncoast systems platform

## Broker Security
Kafka client traffic on `kafka.kafka.svc.internal.lan:9092` is configured for:

- `SASL_PLAINTEXT`
- mechanism `SCRAM-SHA-256`

An internal broker listener (`INTERNAL://:9094`) remains plaintext for broker-internal/bootstrap operations.
ACL authorizer is enabled with `allow.everyone.if.no.acl.found=true`, and bootstrap ACLs are created for admin/connect/schema-registry/ui/graphql-async principals.

## Credential Bootstrap
`manifests/` includes:

- Security bootstrap job `kafka-security-bootstrap` that idempotently creates/rotates Kafka SCRAM users
- Kubernetes secret `kafka-security-credentials` (Vault placeholder backed) for workload env injection, using one-secret-per-key format.

Create these Vault KVv2 paths (each with field `value`):

- `secret/data/kafka-admin-username`
- `secret/data/kafka-admin-password`
- `secret/data/kafka-connect-username`
- `secret/data/kafka-connect-password`
- `secret/data/kafka-schema-registry-username`
- `secret/data/kafka-schema-registry-password`
- `secret/data/kafka-ui-username`
- `secret/data/kafka-ui-password`
- `secret/data/graphql-kafka-async-username`
- `secret/data/graphql-kafka-async-password`
- `secret/data/kafka-nifi-username`
- `secret/data/kafka-nifi-password`
- `secret/data/kafka-flink-username`
- `secret/data/kafka-flink-password`
- `secret/data/kafka-camel-username`
- `secret/data/kafka-camel-password`
- `secret/data/k8s-kafka-keycloak-internal-oidc-issuer-url`
- `secret/data/k8s-kafka-keycloak-internal-oidc-discovery-url`
- `secret/data/keycloak-client-id-kafka-gui-proxy`
- `secret/data/keycloak-client-secret-kafka-gui-proxy`
- `secret/data/k8s-kafka-kafka-gui-keycloak-cookiesecret`
- `secret/data/keycloak-client-id-nifi-gui`
- `secret/data/keycloak-client-secret-nifi-gui`
- `secret/data/k8s-kafka-nifi-keystore-password`
- `secret/data/keycloak-client-id-flink-gui-proxy`
- `secret/data/keycloak-client-secret-flink-gui-proxy`
- `secret/data/k8s-kafka-flink-gui-keycloak-cookiesecret`

## Batch Processing Platform
`manifests/batch-processing-platform.yaml` adds a baseline batch platform in the existing `kafka` namespace:

- Apache NiFi (`StatefulSet`) for scheduled ingestion/orchestration
- Apache Flink native session cluster (`FlinkDeployment` managed by the Flink Kubernetes Operator) for distributed batch compute
- Shared Kafka connection config (`batch-kafka-config`) and SCRAM credentials for NiFi/Flink

Flink runs in native mode to keep idle resource use low. When no jobs are running, the Flink UI can legitimately show zero TaskManagers and `Available Task Slots: 0`; TaskManager pods are allocated dynamically when submitted jobs need slots.

`manifests/nifi-kubernetes-operator.yaml` installs NiFiKop (CRDs + operator) so NiFi flows can be managed declaratively via Kubernetes custom resources.
Current default watch scope is `kafka` namespace only (safe with existing Argo project boundaries).

Kafka bootstrap also provisions ACLs for:

- `kafka-nifi-*` principal for `batch.*` topics
- `kafka-flink-*` principal for `batch.*` topics
- `kafka-camel-*` principal for `batch.*` topics (for Camel routes/connectors you run in Kafka Connect or separate runtimes)

Account model:

- `kafka-*` credentials are robot/service principals for workload-to-Kafka auth
- human UI access uses Keycloak only:
  - Kafka UI and Flink UI use Keycloak-gated `oauth2-proxy`
  - NiFi UI uses native NiFi OIDC with Keycloak

Keycloak authorization:

- Kafka UI, NiFi, and Flink are internal apps and should authenticate against the `internal` realm
- create internal realm role `platform_batch_internal_ui_user`
- assign `platform_batch_internal_ui_user` to users/groups that should access Kafka UI / NiFi / Flink
- configure Keycloak mappers so users receive the `groups` claim containing `platform_batch_internal_ui_user` for NiFi policy group matching
- each UI has its own Keycloak OIDC client credentials secret paths listed above

NiFi 2.x is HTTPS-only and OIDC-enabled; UI runs on `https://nifi-gui.teleport.app.suncoast.systems/nifi`.
Allow these Keycloak redirect URIs for NiFi:
- `https://nifi-gui.teleport.app.suncoast.systems/nifi-api/access/oidc/callback`
- `https://nifi-gui.teleport.app.suncoast.systems:443/nifi-api/access/oidc/callback`
- `https://nifi-gui.teleport.app.suncoast.systems/nifi-api/access/oidc/callback/consumer` (compatibility)

## Batch Examples
Reference dataflow workloads are managed outside this platform repo.

- app-of-apps repo: `https://github.com/dotcomrow/dataflow-apps`
- example workload repo: `https://github.com/dotcomrow/dataflow-example-app`

`k8s-kafka` provides the runtime platform (Kafka, NiFi, Flink, security).  
Pipeline/job manifests should be deployed via the dataflow app-of-apps layer.

## Declarative NiFi Flows
NiFi flow lifecycle is handled with NiFiKop resources in the dataflow workload repo:

- `NifiCluster` (external cluster reference to existing NiFi runtime)
- `NifiRegistryClient`
- `NifiParameterContext`
- `NifiDataflow`

The starter CR templates are stored in `dataflow-example-app/templates/nifi-declarative-flow-crs.yaml`.

Important:

- NiFiKop external-cluster mode requires non-interactive NiFi API auth (`basic` or `tls`) for automation.
- if you use `basic`, provide `username`, `password`, and `ca.crt` in the referenced Kubernetes secret.
- `bucketId` and `flowId` in `NifiDataflow` come from the versioned flow metadata (`bucket.yml` / flow definition metadata).
- to manage CRs in `dataflow` namespace, update operator watch namespaces and Argo project destination allow-lists accordingly.

## Kafka Connectivity Model
Both NiFi and Flink use the same shared Kafka config and their own per-workload credentials:

- shared endpoint/mechanism:
  - `BATCH_KAFKA_BOOTSTRAP_SERVERS` / `KAFKA_BOOTSTRAP_SERVERS` = `kafka.kafka.svc.internal.lan:9092`
  - `BATCH_KAFKA_SECURITY_PROTOCOL` / `KAFKA_SECURITY_PROTOCOL` = `SASL_PLAINTEXT`
  - `BATCH_KAFKA_SASL_MECHANISM` / `KAFKA_SASL_MECHANISM` = `SCRAM-SHA-256`
- NiFi principal:
  - username/password from `kafka-security-credentials` keys `nifi_username` / `nifi_password`
- Flink principal:
  - username/password from `kafka-security-credentials` keys `flink_username` / `flink_password`
- ACL scope:
  - bootstrap grants both principals access to `batch.*` topics.

## Dynamic SCRAM User Reconciliation
Dynamic Kafka accounts are reconciled continuously by CronJob `kafka-security-reconciler` (every five minutes, non-overlapping runs via `concurrencyPolicy: Forbid`).

Safety behavior for eventual Vault consistency:

- Reconciler only applies a user when both username and password keys exist and both `value` fields are non-empty.
- `VAULT_MIN_SECRET_AGE_SECONDS` (default `120`) delays apply until secrets are stable for at least that age.
- Kafka admin CLI calls use `KAFKA_CLI_TIMEOUT=75s` with `KAFKA_AUTH_MAX_RETRIES=3` to tolerate slower broker responses without indefinite retries.
- Critical Kafka config updates retry transient failures via `KAFKA_COMMAND_MAX_RETRIES=3` (e.g., brief DNS resolution blips).
- Kafka bootstrap uses service IP env (`KAFKA_SERVICE_HOST:KAFKA_SERVICE_PORT`) by default (`KAFKA_USE_SERVICE_IP_BOOTSTRAP=true`) to avoid DNS-only failure modes.
- Reconcile job hard timeout is `activeDeadlineSeconds=900` to avoid false failures during slower broker/Vault periods.
- If `VAULT_SKIP_PLACEHOLDER_VALUES=true`, placeholder values are ignored using `VAULT_PLACEHOLDER_VALUES` CSV.
- While any user is in a transitional/invalid state, stale-user deletion is skipped for safety.
- On each successful reconcile, SCRAM password is re-applied from Vault so password rotations converge automatically.

- Create/update user with two Vault KVv2 secrets (both using field `value`):
  - `secret/data/kafka-scram-user-<username>` (Kafka principal username)
  - `secret/data/kafka-scram-user-<username>-password` (SCRAM password)
- Delete user by deleting both keys:
  - `vault kv delete secret/kafka-scram-user-<username>`
  - `vault kv delete secret/kafka-scram-user-<username>-password`

This uses root-level keys under `secret/data/` (no nested folder required).
Only keys with prefix `kafka-scram-user-` are managed by this reconciler.

Optional per-user override keys (all field `value`) are supported:

- `secret/data/kafka-scram-user-<username>-scram-iterations` (default `4096`)
- `secret/data/kafka-scram-user-<username>-resource-pattern-type` (`literal` or `prefixed`)
- `secret/data/kafka-scram-user-<username>-topic-all` (CSV topics, grants `All`)
- `secret/data/kafka-scram-user-<username>-topic-read` (CSV topics, grants `Read`)
- `secret/data/kafka-scram-user-<username>-topic-write` (CSV topics, grants `Write`)
- `secret/data/kafka-scram-user-<username>-topic-describe` (CSV topics, grants `Describe`)
- `secret/data/kafka-scram-user-<username>-group-read` (CSV groups, grants `Read`)
- `secret/data/kafka-scram-user-<username>-group-describe` (CSV groups, grants `Describe`)
- `secret/data/kafka-scram-user-<username>-cluster-describe` (`true/false`)

If override keys are not set, default ACLs are applied for async GraphQL flow:

- topics `graphql.async.requests.v1,graphql.async.responses.v1,graphql.async.responses.dlq.v1` (`All`)
- group `graphql-async-workers` (`Read`,`Describe`)

Example:

```sh
vault kv put secret/kafka-scram-user-ollama-async value='ollama-async'
vault kv put secret/kafka-scram-user-ollama-async-password value='replace-me'
```

CMS content translation producer:

`yb-kafka-vault-config` seeds `cms-content-resolver` idempotently in Vault, including a generated password when missing and write/describe ACL metadata for `batch.cms.content.translation.requests.v1`.

NiFi already has prefixed `batch.*` ACLs from bootstrap, so it can consume `batch.cms.content.translation.requests.v1` and write `batch.cms.content.translation.requests.dlq.v1`.

## Verify
```sh
kubectl -n kafka logs job/kafka-security-bootstrap --tail=200
kubectl -n kafka get jobs -l cronjob-name=kafka-security-reconciler --sort-by=.metadata.creationTimestamp
kubectl -n kafka logs job/$(kubectl -n kafka get jobs -l cronjob-name=kafka-security-reconciler -o jsonpath='{.items[-1:].metadata.name}') --tail=200
kubectl -n kafka exec kafka-0 -- /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-0.kafka-hs.kafka.svc.internal.lan:9094 --describe --entity-type users
kubectl -n kafka get pods -l app=nifi
kubectl -n kafka get pods -l app=flink
kubectl -n kafka exec deploy/flink -- wget -qO- http://localhost:8081/overview
kubectl -n kafka get svc oauth2-proxy nifi flink-oauth2-proxy
```
