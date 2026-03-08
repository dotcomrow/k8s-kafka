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

## Dynamic SCRAM User Reconciliation
Dynamic Kafka accounts are reconciled continuously by CronJob `kafka-security-reconciler` (every minute).

- Create/update user by writing Vault KVv2 secret:
  - `secret/data/kafka-scram-user-<username>`
- Delete user by deleting that Vault key:
  - `vault kv delete secret/kafka-scram-user-<username>`

This uses root-level keys under `secret/data/` (no nested folder required).
For safety, only keys with prefix `kafka-scram-user-` and usernames with prefix `dyn-` are managed by this reconciler.

Supported secret fields for each dynamic user:

- `password` (required)
- `scram_iterations` (optional, default `4096`)
- `resource_pattern_type` (optional: `literal` or `prefixed`, default `literal`)
- `topic_all` (optional CSV, grants topic `All`)
- `topic_read` (optional CSV, grants topic `Read`)
- `topic_write` (optional CSV, grants topic `Write`)
- `topic_describe` (optional CSV, grants topic `Describe`)
- `group_read` (optional CSV, grants group `Read`)
- `group_describe` (optional CSV, grants group `Describe`)
- `cluster_describe` (optional `true/false`, grants cluster `Describe`)

Example:

```sh
vault kv put secret/kafka-scram-user-dyn-analytics-worker \
  password='replace-me' \
  topic_read='graphql.async.requests.v1' \
  topic_write='graphql.async.responses.v1' \
  group_read='graphql-async-workers' \
  group_describe='graphql-async-workers' \
  cluster_describe='true'
```

## Verify
```sh
kubectl -n kafka logs job/kafka-security-bootstrap --tail=200
kubectl -n kafka get jobs -l cronjob-name=kafka-security-reconciler --sort-by=.metadata.creationTimestamp
kubectl -n kafka logs job/$(kubectl -n kafka get jobs -l cronjob-name=kafka-security-reconciler -o jsonpath='{.items[-1:].metadata.name}') --tail=200
kubectl -n kafka exec kafka-0 -- /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-0.kafka-hs.kafka.svc.internal.lan:9094 --describe --entity-type users
```
