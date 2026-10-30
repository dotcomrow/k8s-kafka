# Kafka three-broker / RF3 rollout

This cluster started as a single combined KRaft broker/controller on Apache
Kafka 3.9.0. Its live metadata quorum is static (`kraft.version=0`) with node 1
as the only voter. Kafka 3.9 cannot safely add voters to that static quorum.
Kafka 4.1 added the supported static-to-dynamic quorum conversion used here.

Primary references:

- <https://kafka.apache.org/41/operations/kraft/#upgrade>
- <https://kafka.apache.org/41/getting-started/upgrade/>

The rollout is deliberately gated. Never skip a gate or change `replicas`
directly from 1 to 3 while `controller.quorum.voters` is still configured.

## Target state

- Apache Kafka 4.1.2
- three combined broker/controller pods, one per Kubernetes node
- dynamic KRaft controller membership (`kraft.version=1`)
- three controller voters, tolerating one controller failure
- application and Kafka internal topics at replication factor 3
- `min.insync.replicas=2` for replicated data topics
- broker default retention of 48 hours and the existing seven-day application
  topic ceiling

Combined broker/controllers are retained because this is a small,
CPU-constrained cluster. Separate controller-only JVMs would consume more CPU
and memory. The pods must be spread across three failure domains.

## Gate 0 - preflight

Before every gate:

1. Confirm the Argo CD `kafka` application is Healthy and Synced.
2. Confirm all operation and platform consumer groups have acceptable lag.
3. Confirm no managed-app deployment, notification delivery, or other durable
   operation is in flight.
4. Record the current Git revision, broker image, metadata quorum status,
   feature levels, topic descriptions, topic configuration, consumer offsets,
   and ACLs.
5. Confirm the Kafka PVC is healthy. A recent Longhorn backup/snapshot is an
   optional rollback convenience when retained Kafka data must be preserved;
   it is not required when the accepted recovery path is a declarative rebuild
   with loss of retained messages and consumer offsets.

Run `scripts/kafka-rf3-preflight.sh` to capture the non-secret Kubernetes,
quorum, topic, and consumer-group state before and after each gate.

The rollout changes the metadata quorum. A green Argo application by itself is
not sufficient verification.

## Gate 1 - broker software upgrade

The first source change updates only the existing broker and storage formatter
from Kafka 3.9.0 to 4.1.2. It keeps:

- `replicas: 1`
- the existing static `controller.quorum.voters` configuration
- `kraft.version=0`
- the existing topic replication factors

The StatefulSet uses `RollingUpdate`, so Argo will replace the single broker.
A brief Kafka outage is expected. Do not run a KRaft feature upgrade during the
same rollout.

Verify after the pod is Ready:

```sh
kubectl -n kafka get pod kafka-0 -o wide
kubectl -n kafka get pod kafka-0 -o jsonpath='{.spec.containers[?(@.name=="broker")].image}{"\n"}'
kubectl -n kafka exec kafka-0 -- /opt/kafka/bin/kafka-features.sh \
  --bootstrap-server kafka-0.kafka-hs.kafka.svc.internal.lan:9094 describe
kubectl -n kafka exec kafka-0 -- /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka-0.kafka-hs.kafka.svc.internal.lan:9094 describe --status
```

Expected: image `apache/kafka:4.1.2`, `kraft.version=0`, leader 1, voter 1,
and all existing topics and consumer groups intact.

Gate 1 can be rolled back to the previous Kafka 3.9.0 image because the KRaft
feature and metadata levels have not changed. Do not use that rollback after
Gate 2.

## Gate 2 - convert the existing quorum to dynamic membership

This gate is performed only after Gate 1 is verified. Upgrade only the KRaft
feature level; do not finalize all Kafka 4.1 metadata features at the same time:

```sh
kubectl -n kafka exec kafka-0 -- /opt/kafka/bin/kafka-features.sh \
  --bootstrap-server kafka-0.kafka-hs.kafka.svc.internal.lan:9094 \
  upgrade --feature kraft.version=1
```

Verify `FinalizedVersionLevel: 1`. This conversion is a distinct, explicitly
approved production mutation.

After this succeeds, Kafka 3.9 is no longer a valid rollback target. Recovery
must use Kafka 4.1.2 and the dynamic quorum configuration, restore a
pre-conversion storage backup if one exists, or rebuild the cluster when loss
of retained messages and consumer offsets is acceptable.

After the feature conversion, update the StatefulSet configuration to replace
`controller.quorum.voters` with `controller.quorum.bootstrap.servers`. Roll and
verify kafka-0 before adding replicas. Existing formatted storage must not be
reformatted. The controller headless service must publish not-ready addresses
so each controller's stable DNS name resolves during quorum bootstrap, before
the broker readiness probe succeeds. The Apache Docker wrapper's unconditional
storage setup is bypassed after this conversion because dynamic formatting
requires an explicit controller role; the init container owns formatting and
uses `--standalone` only when kafka-0 storage is genuinely blank.

## Gate 3 - add controllers and brokers 2 and 3

New controller storage must be formatted with `--no-initial-controllers` and
the existing cluster ID. Each pod derives a unique node ID from its StatefulSet
ordinal:

- kafka-0 -> node 1
- kafka-1 -> node 2
- kafka-2 -> node 3

Retain the existing `OrderedReady` pod-management policy because the field is
immutable on the live StatefulSet and sequential startup is safer for this
rollout. Use required pod anti-affinity on `kubernetes.io/hostname`, and
constrain placement to the two worker nodes plus `srvr-node`. Do not schedule a
broker on `ctrl-node` or `etcd-node`. Longhorn replicas remain single-copy and
can be scheduled only on the T4 and L4 worker disks.

Start kafka-1 and kafka-2 as dynamic quorum observers, one StatefulSet ordinal
at a time. Each pod derives its node ID and advertised listeners from its
ordinal. A manifest-owned background reconciler waits for observer lag zero and
runs `kafka-metadata-quorum.sh add-controller` using that pod's local server
configuration. Controller 3 additionally waits until controller 2 is a voter,
which makes the add sequence declarative and safe on a clean rebuild.
The final bootstrap lists contain all three controller and broker endpoints so
membership discovery and reconciliation do not depend on kafka-0 availability.

Gate 3 is complete only when all three pods are Ready and `describe --status`
shows voters 1, 2, and 3 with no material follower lag.

## Gate 4 - raise replication

Only after the three-voter quorum is healthy:

1. Set broker defaults and Kafka internal topic settings to replication factor
   3, with transaction and data-topic minimum ISR 2.
2. Update Kafka Connect, Schema Registry, and repository-owned topic creation
   manifests so new topics are created at RF3.
3. Generate and review an explicit reassignment for every existing topic,
   including compacted and Kafka internal topics. Changing defaults does not
   change existing topics.
4. Execute the reassignment in bounded batches and verify ISR after every
   batch.
5. Set `min.insync.replicas=2` only after all replicas are in sync.

Expected steady state: every applicable partition has three replicas and three
in-sync replicas. The cluster must continue accepting operations after any one
broker is stopped.

## Final regression

Run the internal application operation probes and one harmless Kafka echo
operation. Verify application deploy tracking, notification queuing, runtime
canary execution, realtime operation updates, consumer lag, broker logs, and
retention configuration. Then stop one non-leader broker and repeat the echo
probe before restoring it.

Do not claim the rollout complete from pod readiness or Argo health alone.
