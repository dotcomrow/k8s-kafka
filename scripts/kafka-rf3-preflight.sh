#!/usr/bin/env bash
set -euo pipefail

namespace="${KAFKA_NAMESPACE:-kafka}"
pod="${KAFKA_POD:-kafka-0}"
bootstrap="${KAFKA_BOOTSTRAP_SERVERS:-kafka-0.kafka-hs.kafka.svc.internal.lan:9094}"
detail="${KAFKA_PREFLIGHT_DETAIL:-summary}"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Required command is unavailable: $1" >&2
    exit 1
  fi
}

require_command kubectl
require_command jq

echo "Kubernetes context"
kubectl config current-context

echo "Argo CD Kafka application"
kubectl -n argocd get application kafka -o json | jq '{
  sync: .status.sync.status,
  health: .status.health.status,
  revision: .status.sync.revision,
  operation: (.status.operationState.phase // "none")
}'

echo "Kafka StatefulSet"
kubectl -n "$namespace" get statefulset kafka -o json | jq '{
  desired: .spec.replicas,
  ready: .status.readyReplicas,
  currentRevision: .status.currentRevision,
  updateRevision: .status.updateRevision,
  podManagementPolicy: .spec.podManagementPolicy,
  updateStrategy: .spec.updateStrategy.type,
  brokerImage: (.spec.template.spec.containers[] | select(.name == "broker") | .image)
}'

echo "Kafka pods"
kubectl -n "$namespace" get pods -l app=kafka -o wide

echo "Kafka PVCs"
kubectl -n "$namespace" get pvc -l app=kafka -o wide 2>/dev/null || \
  kubectl -n "$namespace" get pvc -o wide | awk 'NR == 1 || $1 ~ /^kafka-logs-kafka-/'

echo "Kafka pod resource use"
kubectl -n "$namespace" top pods -l app=kafka 2>/dev/null || true

echo "KRaft feature levels"
kubectl -n "$namespace" exec -c broker "$pod" -- \
  /opt/kafka/bin/kafka-features.sh \
  --bootstrap-server "$bootstrap" describe

echo "KRaft metadata quorum"
kubectl -n "$namespace" exec -c broker "$pod" -- \
  /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server "$bootstrap" describe --status

echo "Kafka topics and replicas"
if [[ "$detail" == "full" ]]; then
  kubectl -n "$namespace" exec -c broker "$pod" -- \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$bootstrap" --describe
else
  kubectl -n "$namespace" exec -c broker "$pod" -- \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$bootstrap" --describe | awk '/^Topic: /'
fi

echo "Kafka consumer groups"
kubectl -n "$namespace" exec -c broker "$pod" -- \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server "$bootstrap" --describe --all-groups
