#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="kafka"
CLI_POD="kafka-0"
BOOTSTRAP="kafka-0.kafka-hs.kafka.svc.internal.lan:9094,kafka-1.kafka-hs.kafka.svc.internal.lan:9094,kafka-2.kafka-hs.kafka.svc.internal.lan:9094"
TOPIC="gate4.failover.verify.$(date +%s)"
TOKEN_ONE="gate4-before-$(date +%s%N)"
TOKEN_TWO="gate4-during-$(date +%s%N)"
TOPIC_CREATED=false

kafka_exec() {
  kubectl -n "$NAMESPACE" exec -c broker "$CLI_POD" -- \
    "/opt/kafka/bin/$1" --bootstrap-server "$BOOTSTRAP" "${@:2}"
}

cleanup() {
  if [[ "$TOPIC_CREATED" == "true" ]]; then
    kafka_exec kafka-topics.sh --delete --topic "$TOPIC" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

leader_id="$(kafka_exec kafka-metadata-quorum.sh describe --status \
  | awk '$1 == "LeaderId:" { print $2 }')"
if [[ "$leader_id" != "2" ]]; then
  TARGET_POD="kafka-1"
  TARGET_NODE_ID=2
else
  TARGET_POD="kafka-2"
  TARGET_NODE_ID=3
fi
echo "Controller leader is ${leader_id}; testing loss of non-leader ${TARGET_NODE_ID} (${TARGET_POD})"

kafka_exec kafka-topics.sh --create --topic "$TOPIC" --partitions 1 \
  --replication-factor 3 --config min.insync.replicas=2 >/dev/null
TOPIC_CREATED=true

for _ in $(seq 1 60); do
  description="$(kafka_exec kafka-topics.sh --describe --topic "$TOPIC")"
  if printf '%s\n' "$description" | awk '
    /Partition:/ && !/PartitionCount:/ {
      for (i=1; i<=NF; i++) if ($i == "Isr:") isr=$(i+1)
      if (split(isr, ids, ",") == 3) ready=1
    }
    END { exit !ready }
  '; then
    break
  fi
  sleep 2
done

printf '%s\n' "$TOKEN_ONE" | kubectl -n "$NAMESPACE" exec -i -c broker "$CLI_POD" -- \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" \
  --topic "$TOPIC" --producer-property acks=all >/dev/null

kubectl -n "$NAMESPACE" delete pod "$TARGET_POD" --wait=false >/dev/null
for _ in $(seq 1 90); do
  ready="$(kubectl -n "$NAMESPACE" get pod "$TARGET_POD" \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)"
  [[ "$ready" != "True" ]] && break
  sleep 1
done

for _ in $(seq 1 60); do
  reachable="$(kafka_exec kafka-broker-api-versions.sh 2>/dev/null \
    | awk '/id: [0-9]+ rack:/ { count++ } END { print count+0 }' || true)"
  [[ "$reachable" == "2" ]] && break
  sleep 1
done
if [[ "${reachable:-0}" != "2" ]]; then
  echo "Did not observe the expected two-broker window" >&2
  exit 1
fi

printf '%s\n' "$TOKEN_TWO" | kubectl -n "$NAMESPACE" exec -i -c broker "$CLI_POD" -- \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" \
  --topic "$TOPIC" --producer-property acks=all >/dev/null

consumer_output="$(kubectl -n "$NAMESPACE" exec -c broker "$CLI_POD" -- \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" \
  --topic "$TOPIC" --from-beginning --max-messages 2 --timeout-ms 30000 2>/dev/null)"
grep -Fxq "$TOKEN_ONE" <<<"$consumer_output"
grep -Fxq "$TOKEN_TWO" <<<"$consumer_output"

kubectl -n "$NAMESPACE" wait --for=condition=Ready "pod/${TARGET_POD}" --timeout=300s >/dev/null
for _ in $(seq 1 90); do
  description="$(kafka_exec kafka-topics.sh --describe --topic "$TOPIC")"
  if printf '%s\n' "$description" | awk '
    /Partition:/ && !/PartitionCount:/ {
      for (i=1; i<=NF; i++) if ($i == "Isr:") isr=$(i+1)
      if (split(isr, ids, ",") == 3) ready=1
    }
    END { exit !ready }
  '; then
    break
  fi
  sleep 2
done

echo "RF3 failover passed: acks=all write and read succeeded with broker ${TARGET_NODE_ID} offline"
