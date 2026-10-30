#!/usr/bin/env bash
set -euo pipefail

MODE="${1:---plan}"
case "$MODE" in
  --plan|--execute|--apply-min-isr) ;;
  *)
    echo "Usage: $0 [--plan|--execute|--apply-min-isr]" >&2
    exit 2
    ;;
esac

# Run from a broker so the migration uses the private INTERNAL listener and
# never copies Kafka administrator credentials onto the operator workstation.
# Pass the program as an argument instead of stdin because Kafka CLI processes
# may otherwise consume the remaining input intended for the remote shell.
REMOTE_SCRIPT="$(cat <<'REMOTE_SCRIPT_BODY'
set -euo pipefail

MODE="$1"
BOOTSTRAP="kafka-0.kafka-hs.kafka.svc.internal.lan:9094,kafka-1.kafka-hs.kafka.svc.internal.lan:9094,kafka-2.kafka-hs.kafka.svc.internal.lan:9094"

kafka_cli() {
  local script="$1"
  shift
  "/opt/kafka/bin/${script}" --bootstrap-server "$BOOTSTRAP" "$@" </dev/null
}

broker_count="$(kafka_cli kafka-broker-api-versions.sh 2>/dev/null \
  | awk '/id: [0-9]+ rack:/ { count++ } END { print count+0 }')"
if [[ "$broker_count" -ne 3 ]]; then
  echo "Expected 3 reachable brokers, found ${broker_count}" >&2
  exit 1
fi

verify_topic() {
  local topic="$1"
  kafka_cli kafka-topics.sh --describe --topic "$topic" | awk '
    /Partition:/ {
      replicas=""; isr=""
      for (i=1; i<=NF; i++) {
        if ($i == "Replicas:") replicas=$(i+1)
        if ($i == "Isr:") isr=$(i+1)
      }
      replica_count=split(replicas, replica_ids, ",")
      isr_count=split(isr, isr_ids, ",")
      if (replica_count != 3 || isr_count != 3) bad=1
    }
    END { exit bad }
  '
}

topic_index=0
topic_count=0
migration_count=0
while IFS= read -r topic; do
  [[ -n "$topic" ]] || continue
  topic_count=$((topic_count + 1))
  description="$(kafka_cli kafka-topics.sh --describe --topic "$topic")"
  replication_factor="$(printf '%s\n' "$description" \
    | awk 'NR == 1 { for (i=1; i<=NF; i++) if ($i == "ReplicationFactor:") { print $(i+1); exit } }')"
  partition_count="$(printf '%s\n' "$description" \
    | awk 'NR == 1 { for (i=1; i<=NF; i++) if ($i == "PartitionCount:") { print $(i+1); exit } }')"
  printf '%-55s partitions=%-3s RF=%s\n' "$topic" "$partition_count" "$replication_factor"

  if [[ "$MODE" == "--execute" && "$replication_factor" -lt 3 ]]; then
    assignment="/tmp/gate4-reassignment-${topic_index}.json"
    printf '%s\n' "$description" | awk -v topic="$topic" -v offset="$topic_index" '
      BEGIN { printf "{\"version\":1,\"partitions\":["; first=1 }
      /Partition:/ {
        partition=-1
        for (i=1; i<=NF; i++) if ($i == "Partition:") partition=$(i+1)
        if (partition < 0) next
        preferred=(partition + offset) % 3 + 1
        second=preferred % 3 + 1
        third=second % 3 + 1
        if (!first) printf ","
        printf "{\"topic\":\"%s\",\"partition\":%d,\"replicas\":[%d,%d,%d]}", topic, partition, preferred, second, third
        first=0
      }
      END { print "]}" }
    ' > "$assignment"

    kafka_cli kafka-reassign-partitions.sh \
      --reassignment-json-file "$assignment" --execute

    until verify_topic "$topic"; do
      kafka_cli kafka-reassign-partitions.sh \
        --reassignment-json-file "$assignment" --verify || true
      sleep 5
    done
    kafka_cli kafka-reassign-partitions.sh \
      --reassignment-json-file "$assignment" --verify
    migration_count=$((migration_count + 1))
  fi

  topic_index=$((topic_index + 1))
done < <(kafka_cli kafka-topics.sh --list | sort)

if [[ "$MODE" == "--apply-min-isr" ]]; then
  while IFS= read -r topic; do
    [[ -n "$topic" ]] || continue
    if ! verify_topic "$topic"; then
      echo "Refusing to set min ISR: ${topic} does not have three in-sync replicas" >&2
      exit 1
    fi
  done < <(kafka_cli kafka-topics.sh --list | sort)

  while IFS= read -r topic; do
    [[ -n "$topic" ]] || continue
    kafka_cli kafka-configs.sh --alter --entity-type topics \
      --entity-name "$topic" --add-config min.insync.replicas=2
  done < <(kafka_cli kafka-topics.sh --list | sort)
fi

echo "Topics checked: ${topic_count}; topics migrated: ${migration_count}; mode: ${MODE}"
REMOTE_SCRIPT_BODY
)"

kubectl -n kafka exec -c broker kafka-0 -- bash -lc "$REMOTE_SCRIPT" _ "$MODE"
