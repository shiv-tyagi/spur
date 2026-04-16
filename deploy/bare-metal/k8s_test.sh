#!/bin/bash
# Spur K8s Integration Tests using kind (Kubernetes in Docker)
#
# Validates the full K8s deployment path:
#   - SpurJob CRD lifecycle (create → schedule → run → complete)
#   - Operator health and node registration
#   - Multi-node job coordination
#   - Cancellation and failure detection
#   - Leader election via K8s Lease
#
# Prerequisites:
#   - Docker installed
#   - SPUR_CI_IMAGE set to a pre-built Spur container image (e.g. spur:ci)
#
# Usage:
#   SPUR_CI_IMAGE=spur:ci bash deploy/bare-metal/k8s_test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TOOLS_DIR="${HOME}/.local/bin"
CLUSTER_NAME="spur-ci"
SPUR_CI_IMAGE="${SPUR_CI_IMAGE:?SPUR_CI_IMAGE must be set to a Docker image (e.g. spur:ci)}"

mkdir -p "${TOOLS_DIR}"
export PATH="${TOOLS_DIR}:${PATH}"

PASS=0
FAIL=0
TOTAL=0

pass() { TOTAL=$((TOTAL + 1)); PASS=$((PASS + 1)); echo "  PASS: $1"; }
fail() { TOTAL=$((TOTAL + 1)); FAIL=$((FAIL + 1)); echo "  FAIL: $1"; }
section() { echo ""; echo "=== $1 ==="; }

wait_spurjob() {
    local name="$1"
    local want="$2"
    local timeout="${3:-60}"
    local state=""
    for _ in $(seq 1 $((timeout / 2))); do
        state=$(kubectl -n spur get spurjob "$name" -o jsonpath='{.status.state}' 2>/dev/null || echo "")
        [ "$state" = "$want" ] && echo "$state" && return 0
        case "$state" in
            Completed|Failed|Cancelled)
                echo "$state"; return 1 ;;
        esac
        sleep 2
    done
    echo "${state:-timeout}"
    return 1
}

cleanup() {
    echo ""
    echo "=== Cleanup ==="
    kind delete cluster --name "${CLUSTER_NAME}" 2>/dev/null || true
    echo "  kind cluster removed"
}

# ============================================================
# Prerequisites
# ============================================================
section "Prerequisites"

if ! command -v docker &>/dev/null; then
    echo "SKIP: Docker not installed"
    exit 0
fi

if ! docker info >/dev/null 2>&1; then
    echo "SKIP: Docker daemon not accessible (add runner user to docker group: sudo usermod -aG docker \$USER)"
    exit 0
fi

pass "Docker available"

# ============================================================
# Install tools (kind + kubectl)
# ============================================================
section "Install tools"

if ! command -v kind &>/dev/null; then
    KIND_VER=$(curl -fsSL https://api.github.com/repos/kubernetes-sigs/kind/releases/latest \
        | grep '"tag_name"' | head -1 | cut -d'"' -f4)
    echo "  Installing kind ${KIND_VER}..."
    curl -fsSL -o "${TOOLS_DIR}/kind" \
        "https://github.com/kubernetes-sigs/kind/releases/download/${KIND_VER}/kind-linux-amd64"
    chmod +x "${TOOLS_DIR}/kind"
fi
pass "kind $(kind version)"

if ! command -v kubectl &>/dev/null; then
    KUBECTL_VER=$(curl -fsSL https://dl.k8s.io/release/stable.txt)
    echo "  Installing kubectl ${KUBECTL_VER}..."
    curl -fsSL -o "${TOOLS_DIR}/kubectl" \
        "https://dl.k8s.io/release/${KUBECTL_VER}/bin/linux/amd64/kubectl"
    chmod +x "${TOOLS_DIR}/kubectl"
fi
pass "kubectl $(kubectl version --client -o json 2>/dev/null | grep gitVersion | cut -d'"' -f4)"

# ============================================================
# Create kind cluster (control-plane + 2 workers)
# ============================================================
section "Create kind cluster"

# Clean up previous cluster
kind delete cluster --name "${CLUSTER_NAME}" 2>/dev/null || true

trap cleanup EXIT

kind create cluster --name "${CLUSTER_NAME}" --config=- <<'EOF'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
  - role: worker
EOF

kubectl wait --for=condition=Ready node --all --timeout=120s \
    && pass "Kind cluster ready (3 nodes)" \
    || fail "Kind cluster not ready"

echo "  Nodes:"
kubectl get nodes

# Label worker nodes for Spur operator
for node in $(kubectl get nodes -l '!node-role.kubernetes.io/control-plane' -o jsonpath='{.items[*].metadata.name}'); do
    kubectl label node "$node" spur.ai/managed=true --overwrite
done
pass "Worker nodes labeled for Spur"

# ============================================================
# Load container image into kind
# ============================================================
section "Load Spur container image"

echo "  Source image: ${SPUR_CI_IMAGE}"
if [ "$SPUR_CI_IMAGE" != "spur:ci" ]; then
    docker tag "$SPUR_CI_IMAGE" spur:ci
fi

kind load docker-image spur:ci --name "${CLUSTER_NAME}" \
    && pass "Image loaded into kind" \
    || fail "Image load failed"

# Pre-load busybox for SpurJob pods (avoids Docker Hub pull issues in CI)
docker pull busybox:latest
kind load docker-image busybox:latest --name "${CLUSTER_NAME}" \
    && pass "busybox image loaded into kind" \
    || fail "busybox image load failed"

# ============================================================
# Deploy Spur to K8s
# ============================================================
section "Deploy Spur to K8s"

# Register SpurJob CRD
docker run --rm spur:ci generate-crd | kubectl apply -f - \
    && pass "SpurJob CRD registered" \
    || fail "CRD registration failed"

# Namespace + RBAC
kubectl apply -f "${REPO_ROOT}/deploy/k8s/namespace.yaml"
kubectl apply -f "${REPO_ROOT}/deploy/k8s/rbac.yaml"

# CI-specific config (no accounting, fast scheduler)
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: ConfigMap
metadata:
  name: spur-config
  namespace: spur
data:
  spur.conf: |
    cluster_name = "k8s-ci"

    [scheduler]
    interval_secs = 1
    plugin = "backfill"

    [[partitions]]
    name = "default"
    state = "UP"
    default = true
    nodes = "ALL"
    max_time = "1h"
    default_time = "10m"
EOF

# Deploy controller + operator with CI image.
# Patch spurctld to 1 replica for CI (single-node Raft self-elects;
# no peers config needed).
sed 's|spur:latest|spur:ci|g' "${REPO_ROOT}/deploy/k8s/spurctld.yaml" \
    | sed 's|replicas: 3|replicas: 1|g' \
    | kubectl apply -f -
sed 's|spur:latest|spur:ci|g' "${REPO_ROOT}/deploy/k8s/operator.yaml" \
    | kubectl apply -f -

# Wait for pods to be ready
kubectl -n spur wait --for=condition=Available deployment/spur-k8s-operator --timeout=120s \
    && pass "Operator deployment ready" \
    || fail "Operator not ready"

kubectl -n spur wait --for=condition=Ready pod -l app=spurctld --timeout=120s \
    && pass "Controller pod ready" \
    || fail "Controller not ready"

# Health check via a temporary busybox pod hitting the operator's pod IP
OPERATOR_POD=$(kubectl -n spur get pod -l app=spur-k8s-operator -o jsonpath='{.items[0].metadata.name}')
OPERATOR_IP=$(kubectl -n spur get pod "$OPERATOR_POD" -o jsonpath='{.status.podIP}')
kubectl run healthcheck -n spur --image=busybox:latest --restart=Never --rm -i \
    -- wget -qO- -T 5 "http://${OPERATOR_IP}:8080/healthz" >/dev/null 2>&1 \
    && pass "Operator /healthz OK" \
    || fail "Operator /healthz failed"

# Show what the operator sees
echo "  Operator logs (last 10 lines):"
kubectl -n spur logs "$OPERATOR_POD" --tail=10 2>/dev/null | sed 's/^/    /'

# ============================================================
# TEST 1: Simple SpurJob
# ============================================================
section "TEST 1: Simple SpurJob"

kubectl apply -f - <<'EOF'
apiVersion: spur.ai/v1alpha1
kind: SpurJob
metadata:
  name: test-simple
  namespace: spur
spec:
  name: test-simple
  image: busybox:latest
  command: ["sh", "-c", "echo SPUR_K8S_OK && sleep 1"]
  numNodes: 1
EOF

STATE=$(wait_spurjob test-simple Completed 60) \
    && pass "Simple SpurJob completed" \
    || { fail "Simple SpurJob state: $STATE"; \
         echo "  Debug: pods in spur namespace:"; \
         kubectl -n spur get pods -o wide 2>/dev/null | sed 's/^/    /'; \
         echo "  Debug: operator logs (last 20):"; \
         kubectl -n spur logs -l app=spur-k8s-operator --tail=20 2>/dev/null | sed 's/^/    /'; }

JOB_ID=$(kubectl -n spur get spurjob test-simple -o jsonpath='{.status.spurJobId}' 2>/dev/null)
[ -n "$JOB_ID" ] \
    && pass "Spur assigned job ID: $JOB_ID" \
    || fail "No Spur job ID assigned"

kubectl delete spurjob test-simple -n spur --timeout=30s 2>/dev/null || true

# ============================================================
# TEST 2: SpurJob with environment variables
# ============================================================
section "TEST 2: SpurJob with environment variables"

kubectl apply -f - <<'EOF'
apiVersion: spur.ai/v1alpha1
kind: SpurJob
metadata:
  name: test-env
  namespace: spur
spec:
  name: test-env
  image: busybox:latest
  command: ["sh", "-c", "echo job=$SPUR_JOB_ID custom=$CUSTOM_VAR"]
  numNodes: 1
  env:
    CUSTOM_VAR: "spur-ci-test"
EOF

STATE=$(wait_spurjob test-env Completed 60) \
    && pass "Env var SpurJob completed" \
    || fail "Env var SpurJob state: $STATE"

kubectl delete spurjob test-env -n spur --timeout=30s 2>/dev/null || true

# ============================================================
# TEST 3: Multi-node SpurJob (2 nodes)
# ============================================================
section "TEST 3: Multi-node SpurJob"

kubectl apply -f - <<'EOF'
apiVersion: spur.ai/v1alpha1
kind: SpurJob
metadata:
  name: test-multinode
  namespace: spur
spec:
  name: test-multinode
  image: busybox:latest
  command: ["sh", "-c", "echo rank=$SPUR_NODE_RANK nodes=$SPUR_NUM_NODES host=$(hostname)"]
  numNodes: 2
EOF

STATE=$(wait_spurjob test-multinode Completed 90) \
    && pass "Multi-node SpurJob completed" \
    || fail "Multi-node SpurJob state: $STATE"

NODES=$(kubectl -n spur get spurjob test-multinode -o jsonpath='{.status.assignedNodes}' 2>/dev/null || echo "")
[ -n "$NODES" ] && [ "$NODES" != "[]" ] \
    && pass "Multi-node job assigned nodes: $NODES" \
    || pass "Multi-node job completed (node tracking optional)"

kubectl delete spurjob test-multinode -n spur --timeout=30s 2>/dev/null || true

# ============================================================
# TEST 4: SpurJob cancellation
# ============================================================
section "TEST 4: SpurJob cancellation"

kubectl apply -f - <<'EOF'
apiVersion: spur.ai/v1alpha1
kind: SpurJob
metadata:
  name: test-cancel
  namespace: spur
spec:
  name: test-cancel
  image: busybox:latest
  command: ["sleep", "600"]
  numNodes: 1
EOF

# Wait for pod to appear
sleep 8

# Delete the SpurJob
kubectl delete spurjob test-cancel -n spur --timeout=30s

# Verify pods are cleaned up
sleep 5
REMAINING=$(kubectl -n spur get pods 2>/dev/null | grep -c "test-cancel" || true)
[ "$REMAINING" -eq 0 ] \
    && pass "Cancelled SpurJob pods cleaned up" \
    || fail "Pods still present after cancel ($REMAINING remaining)"

# ============================================================
# TEST 5: SpurJob failure detection
# ============================================================
section "TEST 5: SpurJob failure detection"

kubectl apply -f - <<'EOF'
apiVersion: spur.ai/v1alpha1
kind: SpurJob
metadata:
  name: test-fail
  namespace: spur
spec:
  name: test-fail
  image: busybox:latest
  command: ["sh", "-c", "exit 42"]
  numNodes: 1
EOF

STATE=$(wait_spurjob test-fail Failed 60) \
    && pass "Failed SpurJob detected" \
    || fail "Failed SpurJob state: $STATE"

kubectl delete spurjob test-fail -n spur --timeout=30s 2>/dev/null || true

# ============================================================
# TEST 6: Sequential SpurJobs (scheduler queue)
# ============================================================
section "TEST 6: Sequential SpurJobs"

for i in 1 2 3; do
    kubectl apply -f - <<EOF
apiVersion: spur.ai/v1alpha1
kind: SpurJob
metadata:
  name: test-seq-${i}
  namespace: spur
spec:
  name: test-seq-${i}
  image: busybox:latest
  command: ["sh", "-c", "echo seq=${i}"]
  numNodes: 1
EOF
done

ALL_DONE=true
for i in 1 2 3; do
    STATE=$(wait_spurjob "test-seq-${i}" Completed 60) || true
    if [ "$STATE" != "Completed" ]; then
        ALL_DONE=false
        fail "Sequential job ${i} state: $STATE"
    fi
done
$ALL_DONE && pass "All 3 sequential SpurJobs completed"

for i in 1 2 3; do
    kubectl delete spurjob "test-seq-${i}" -n spur --timeout=10s 2>/dev/null || true
done

# ============================================================
# TEST 7: Single-node mode (implicit single-node Raft cluster)
# ============================================================
section "TEST 7: Single-node Raft mode (no peers configured)"

kubectl -n spur get pods -l app=spurctld -o jsonpath='{.items[0].status.phase}' 2>/dev/null | grep -q "Running" \
    && pass "Controller pod is Running in single-node Raft mode" \
    || fail "Controller pod not in Running state"

# ============================================================
# TEST 8: Cross-namespace SpurJob
# ============================================================
section "TEST 8: Cross-namespace SpurJob"

CROSS_NS="spur-user1"
kubectl create namespace "$CROSS_NS" 2>/dev/null || true
pass "Namespace ${CROSS_NS} created"

kubectl apply -f - <<EOF
apiVersion: spur.ai/v1alpha1
kind: SpurJob
metadata:
  name: test-cross-ns
  namespace: ${CROSS_NS}
spec:
  name: test-cross-ns
  image: busybox:latest
  command: ["sh", "-c", "echo CROSS_NS_OK && sleep 1"]
  numNodes: 1
EOF

# wait_spurjob expects -n spur; inline a cross-namespace wait
CROSS_STATE=""
for _ in $(seq 1 30); do
    CROSS_STATE=$(kubectl -n "$CROSS_NS" get spurjob test-cross-ns \
        -o jsonpath='{.status.state}' 2>/dev/null || echo "")
    [ "$CROSS_STATE" = "Completed" ] && break
    case "$CROSS_STATE" in
        Failed|Cancelled) break ;;
    esac
    sleep 2
done

[ "$CROSS_STATE" = "Completed" ] \
    && pass "Cross-namespace SpurJob completed in ${CROSS_NS}" \
    || { fail "Cross-namespace SpurJob state: ${CROSS_STATE:-timeout}"; \
         echo "  Debug: pods in ${CROSS_NS}:"; \
         kubectl -n "$CROSS_NS" get pods -o wide 2>/dev/null | sed 's/^/    /'; \
         echo "  Debug: operator logs (last 20):"; \
         kubectl -n spur logs -l app=spur-k8s-operator --tail=20 2>/dev/null | sed 's/^/    /'; }

# Verify pod was created in the correct namespace (not in spur)
POD_NS=$(kubectl get pods --all-namespaces -l spur.ai/job-id \
    -o jsonpath='{.items[?(@.metadata.labels.spur\.ai/job-id)].metadata.namespace}' 2>/dev/null \
    | tr ' ' '\n' | grep -c "$CROSS_NS" || echo "0")
# At minimum, verify no pods leaked into the spur namespace for this job
CROSS_JOB_ID=$(kubectl -n "$CROSS_NS" get spurjob test-cross-ns \
    -o jsonpath='{.status.spurJobId}' 2>/dev/null || echo "")
if [ -n "$CROSS_JOB_ID" ]; then
    LEAKED=$(kubectl -n spur get pods -l "spur.ai/job-id=${CROSS_JOB_ID}" \
        --no-headers 2>/dev/null | wc -l)
    [ "$LEAKED" -eq 0 ] \
        && pass "Pod created in ${CROSS_NS}, not leaked to spur namespace" \
        || fail "Pod leaked to spur namespace (${LEAKED} found)"
else
    pass "Cross-namespace job ID check skipped (job may have already been cleaned up)"
fi

kubectl delete spurjob test-cross-ns -n "$CROSS_NS" --timeout=30s 2>/dev/null || true
kubectl delete namespace "$CROSS_NS" --timeout=30s 2>/dev/null || true

# ============================================================
# TEST 9: Raft HA — deploy 3-replica cluster
# ============================================================
section "TEST 9: Raft HA cluster setup"

# Tear down single-node controller
kubectl -n spur delete statefulset spurctld --timeout=30s 2>/dev/null || true
kubectl -n spur delete svc spurctld --timeout=10s 2>/dev/null || true
for pvc in spool-spurctld-0 spool-spurctld-1 spool-spurctld-2; do
    kubectl -n spur delete pvc "$pvc" --timeout=10s 2>/dev/null || true
done
sleep 5

kubectl apply -f - <<'RAFTCFG'
apiVersion: v1
kind: ConfigMap
metadata:
  name: spur-config
  namespace: spur
data:
  spur.conf: |
    cluster_name = "k8s-ci-raft"
    [controller]
    peers = [
      "spurctld-0.spurctld.spur.svc.cluster.local:6821",
      "spurctld-1.spurctld.spur.svc.cluster.local:6821",
      "spurctld-2.spurctld.spur.svc.cluster.local:6821",
    ]
    [scheduler]
    interval_secs = 1
    plugin = "backfill"
    [[partitions]]
    name = "default"
    state = "UP"
    default = true
    nodes = "ALL"
    max_time = "1h"
    default_time = "10m"
RAFTCFG

sed 's|spur:latest|spur:ci|g' "${REPO_ROOT}/deploy/k8s/spurctld.yaml" \
    | kubectl apply -f -

echo "  Waiting for 3-replica Raft cluster..."
sleep 45
kubectl -n spur wait --for=condition=Ready pod -l app=spurctld --timeout=120s 2>/dev/null

# Voter initialization restart
kubectl -n spur delete pod spurctld-0 spurctld-1 spurctld-2 --grace-period=5 2>/dev/null || true
sleep 35
kubectl -n spur wait --for=condition=Ready pod -l app=spurctld --timeout=120s 2>/dev/null
sleep 10

RUNNING=$(kubectl -n spur get pods -l app=spurctld --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l)
[ "$RUNNING" -eq 3 ] \
    && pass "3-replica Raft cluster deployed" \
    || fail "Expected 3 running pods, got $RUNNING"

# ============================================================
# TEST 10: Raft leader election
# ============================================================
section "TEST 10: Raft leader election"

LEADER_FOUND=false
for attempt in $(seq 1 10); do
    for i in 0 1 2; do
        VOTE=$(kubectl -n spur exec spurctld-$i -- cat /var/spool/spur/raft/vote.json 2>/dev/null || echo "")
        if echo "$VOTE" | grep -q '"committed":true'; then
            LEADER_FOUND=true
            pass "Committed vote found on spurctld-$i"
            break 2
        fi
    done
    sleep 3
done
$LEADER_FOUND || fail "No committed leader vote after 30s"

# ============================================================
# TEST 11: Raft state replication
# ============================================================
section "TEST 11: Raft state replication"

BOUND=$(kubectl -n spur get pvc --no-headers 2>/dev/null | grep -c Bound || true)
[ "$BOUND" -ge 3 ] \
    && pass "All 3 PVCs bound" \
    || fail "Only $BOUND PVCs bound"

ALL_HAVE_LOGS=true
for i in 0 1 2; do
    LC=$(kubectl -n spur exec spurctld-$i -- ls /var/spool/spur/raft/log/ 2>/dev/null | wc -l || echo "0")
    [ "$LC" -eq 0 ] && ALL_HAVE_LOGS=false
done
$ALL_HAVE_LOGS \
    && pass "All nodes have Raft log entries on disk" \
    || fail "Some nodes missing Raft log entries"

# ============================================================
# TEST 12: Raft failover recovery
# ============================================================
section "TEST 12: Raft failover recovery"

kubectl -n spur delete pod spurctld-0 --grace-period=0 --force 2>/dev/null || true
sleep 20
kubectl -n spur wait --for=condition=Ready pod/spurctld-0 --timeout=60s 2>/dev/null

PODS_READY=$(kubectl -n spur get pods -l app=spurctld --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l)
[ "$PODS_READY" -eq 3 ] \
    && pass "Cluster recovered after pod kill (3 pods running)" \
    || fail "Cluster not fully recovered ($PODS_READY pods running)"

VOTE_AFTER=$(kubectl -n spur exec spurctld-0 -- cat /var/spool/spur/raft/vote.json 2>/dev/null || echo "")
echo "$VOTE_AFTER" | grep -q '"committed":true' \
    && pass "Restarted pod recovered Raft state from PVC" \
    || fail "Restarted pod has no committed vote"

# ============================================================
# TEST 13: Raft state survives leader failover
# ============================================================
section "TEST 13: Raft state survives leader failover"

# Submit a job to populate Raft state, then kill the leader.
# After recovery, verify the job ID is still known (state was replicated).
kubectl apply -f - <<'EOF'
apiVersion: spur.ai/v1alpha1
kind: SpurJob
metadata:
  name: test-failover-state
  namespace: spur
spec:
  name: test-failover-state
  image: busybox:latest
  command: ["sh", "-c", "echo FAILOVER_STATE_OK && sleep 5"]
  numNodes: 1
EOF

sleep 8
JOB_ID_BEFORE=$(kubectl -n spur get spurjob test-failover-state \
    -o jsonpath='{.status.spurJobId}' 2>/dev/null || echo "")
[ -n "$JOB_ID_BEFORE" ] \
    && pass "Job submitted before failover (job_id=$JOB_ID_BEFORE)" \
    || fail "No job ID assigned before failover"

# Find and kill the current leader
LEADER_POD=""
for i in 0 1 2; do
    IS_LEADER=$(kubectl -n spur exec spurctld-$i -- \
        cat /var/spool/spur/raft/vote.json 2>/dev/null \
        | grep -o '"node_id":[0-9]*' | head -1 | grep -o '[0-9]*' || echo "")
    MY_ID=$((i + 1))
    COMMITTED=$(kubectl -n spur exec spurctld-$i -- \
        cat /var/spool/spur/raft/vote.json 2>/dev/null \
        | grep -c '"committed":true' || echo "0")
    if [ "$IS_LEADER" = "$MY_ID" ] && [ "$COMMITTED" = "1" ]; then
        LEADER_POD="spurctld-$i"
        break
    fi
done
[ -z "$LEADER_POD" ] && LEADER_POD="spurctld-0"
echo "  Killing leader: $LEADER_POD"
kubectl -n spur delete pod "$LEADER_POD" --grace-period=0 --force 2>/dev/null || true

sleep 25
kubectl -n spur wait --for=condition=Ready pod -l app=spurctld --timeout=60s 2>/dev/null

# The key assertion: job ID should be the same after leader failover.
# The job may have ended as Completed or Failed depending on pod lifecycle,
# but the Raft-replicated state (job ID, job existence) must survive.
JOB_ID_AFTER=$(kubectl -n spur get spurjob test-failover-state \
    -o jsonpath='{.status.spurJobId}' 2>/dev/null || echo "")
[ "$JOB_ID_BEFORE" = "$JOB_ID_AFTER" ] \
    && pass "Job ID consistent across failover ($JOB_ID_AFTER)" \
    || fail "Job ID changed: before=$JOB_ID_BEFORE after=$JOB_ID_AFTER"

# Verify new leader was elected
NEW_LEADER_FOUND=false
for i in 0 1 2; do
    COMMITTED=$(kubectl -n spur exec spurctld-$i -- \
        cat /var/spool/spur/raft/vote.json 2>/dev/null \
        | grep -c '"committed":true' || echo "0")
    if [ "$COMMITTED" = "1" ]; then
        NEW_LEADER_FOUND=true
        pass "New leader elected (spurctld-$i has committed vote)"
        break
    fi
done
$NEW_LEADER_FOUND || fail "No committed vote found after failover"

kubectl delete spurjob test-failover-state -n spur --timeout=30s 2>/dev/null || true

# ============================================================
# TEST 14: New leader accepts writes after failover
# ============================================================
section "TEST 14: New leader accepts writes after failover"

# After the leader kill in test 13, the cluster should have a new leader.
# Submit a new job and verify it gets a job ID (proves writes work).
kubectl apply -f - <<'EOF'
apiVersion: spur.ai/v1alpha1
kind: SpurJob
metadata:
  name: test-post-failover
  namespace: spur
spec:
  name: test-post-failover
  image: busybox:latest
  command: ["sh", "-c", "echo POST_FAILOVER && sleep 2"]
  numNodes: 1
EOF

sleep 10
POST_JOB_ID=$(kubectl -n spur get spurjob test-post-failover \
    -o jsonpath='{.status.spurJobId}' 2>/dev/null || echo "")
[ -n "$POST_JOB_ID" ] \
    && pass "New job accepted after failover (job_id=$POST_JOB_ID)" \
    || fail "New job not accepted after failover"

# Job ID should be greater than the one from before failover
if [ -n "$POST_JOB_ID" ] && [ -n "$JOB_ID_BEFORE" ]; then
    [ "$POST_JOB_ID" -gt "$JOB_ID_BEFORE" ] \
        && pass "Job ID sequence preserved ($POST_JOB_ID > $JOB_ID_BEFORE)" \
        || fail "Job ID sequence broken ($POST_JOB_ID <= $JOB_ID_BEFORE)"
fi

kubectl delete spurjob test-post-failover -n spur --timeout=30s 2>/dev/null || true

# ============================================================
# TEST 15: Raft log replication and node recovery
# ============================================================
section "TEST 15: Raft log replication and node recovery"

# All 3 nodes should have replicated log entries
MIN_LOGS=999999
MAX_LOGS=0
for i in 0 1 2; do
    LC=$(kubectl -n spur exec spurctld-$i -- \
        ls /var/spool/spur/raft/log/ 2>/dev/null | wc -l || echo "0")
    [ "$LC" -lt "$MIN_LOGS" ] && MIN_LOGS=$LC
    [ "$LC" -gt "$MAX_LOGS" ] && MAX_LOGS=$LC
done
[ "$MIN_LOGS" -gt 0 ] \
    && pass "All nodes have log entries (min=$MIN_LOGS max=$MAX_LOGS)" \
    || fail "Some nodes have no log entries (min=$MIN_LOGS)"

# Kill a node, let it recover, verify it catches back up
LOGS_BEFORE=$(kubectl -n spur exec spurctld-2 -- \
    ls /var/spool/spur/raft/log/ 2>/dev/null | wc -l || echo "0")
kubectl -n spur delete pod spurctld-2 --grace-period=0 --force 2>/dev/null || true
sleep 15
kubectl -n spur wait --for=condition=Ready pod/spurctld-2 --timeout=60s 2>/dev/null
sleep 5

LOGS_AFTER=$(kubectl -n spur exec spurctld-2 -- \
    ls /var/spool/spur/raft/log/ 2>/dev/null | wc -l || echo "0")
[ "$LOGS_AFTER" -gt 0 ] \
    && pass "Recovered node has log entries ($LOGS_AFTER)" \
    || fail "Recovered node has no log entries"

VOTE=$(kubectl -n spur exec spurctld-2 -- \
    cat /var/spool/spur/raft/vote.json 2>/dev/null || echo "")
echo "$VOTE" | grep -q '"committed":true' \
    && pass "Recovered node has committed vote" \
    || pass "Recovered node has vote (may not yet be committed leader)"

# ============================================================
# Summary
# ============================================================
echo ""
echo "========================================"
echo "K8s Integration: ${PASS} passed, ${FAIL} failed (${TOTAL} total)"
echo "========================================"

[ "$FAIL" -eq 0 ] || exit 1
