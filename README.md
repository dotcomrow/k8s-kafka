# k8s-kafka
Kafka system and components for Suncoast systems platform

## Broker Security
Kafka client traffic on `kafka.kafka.svc.internal.lan:9092` is configured for:

- `SASL_PLAINTEXT`
- mechanism `SCRAM-SHA-256`

An internal broker listener (`INTERNAL://:9094`) remains plaintext for broker-internal/bootstrap operations.
ACL authorizer is enabled with `allow.everyone.if.no.acl.found=true`, and bootstrap ACLs are created for admin/connect/schema-registry/ui/graphql-async principals.

## Credential Bootstrap
`manifests/k8s-kafka.yaml` includes:

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

## Verify
```sh
kubectl -n kafka logs job/kafka-security-bootstrap --tail=200
kubectl -n kafka exec kafka-0 -- /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-0.kafka-hs.kafka.svc.internal.lan:9094 --describe --entity-type users
```
