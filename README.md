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

## Verify
```sh
kubectl -n kafka logs job/kafka-security-bootstrap --tail=200
kubectl -n kafka get jobs -l cronjob-name=kafka-security-reconciler --sort-by=.metadata.creationTimestamp
kubectl -n kafka logs job/$(kubectl -n kafka get jobs -l cronjob-name=kafka-security-reconciler -o jsonpath='{.items[-1:].metadata.name}') --tail=200
kubectl -n kafka exec kafka-0 -- /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-0.kafka-hs.kafka.svc.internal.lan:9094 --describe --entity-type users
```
