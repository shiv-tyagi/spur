#!/usr/bin/env bash
# Bootstrap a multi-node kubeadm cluster across VMs.
# Expects VMs to already have kubeadm, kubelet, kubectl, and containerd installed.

set -euo pipefail

usage() {
    cat <<EOF
Usage: $(basename "$0") [options]

Options:
  --control-plane IP   Control plane node IP (required)
  --workers IP,IP,...  Comma-separated worker node IPs (required)
  --ssh-key PATH       SSH private key (default: ~/.ssh/id_ed25519)
  --ssh-user USER      SSH user (default: ci)
  --kubeconfig PATH    Where to write KUBECONFIG (default: /tmp/spur-kubeconfig)
  --pod-cidr CIDR      Pod network CIDR (default: 10.244.0.0/16)
  --image-tar PATH     Container image tar to side-load into all nodes (optional)
EOF
    exit 1
}

CP_IP=""
WORKERS=""
SSH_KEY="${HOME}/.ssh/id_ed25519"
SSH_USER="ci"
KUBECONFIG_OUT="/tmp/spur-kubeconfig"
POD_CIDR="10.244.0.0/16"
IMAGE_TAR=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --control-plane) CP_IP="$2"; shift 2 ;;
        --workers) WORKERS="$2"; shift 2 ;;
        --ssh-key) SSH_KEY="$2"; shift 2 ;;
        --ssh-user) SSH_USER="$2"; shift 2 ;;
        --kubeconfig) KUBECONFIG_OUT="$2"; shift 2 ;;
        --pod-cidr) POD_CIDR="$2"; shift 2 ;;
        --image-tar) IMAGE_TAR="$2"; shift 2 ;;
        *) echo "Unknown option: $1" >&2; usage ;;
    esac
done

[[ -z "$CP_IP" ]] && { echo "ERROR: --control-plane required" >&2; usage; }
[[ -z "$WORKERS" ]] && { echo "ERROR: --workers required" >&2; usage; }

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10"

# Pre-flight: verify SSH connectivity to all nodes before proceeding
IFS=',' read -ra _ALL_IPS <<< "$CP_IP,$WORKERS"
for _ip in "${_ALL_IPS[@]}"; do
    for _attempt in $(seq 1 30); do
        if ssh $SSH_OPTS -o BatchMode=yes -i "$SSH_KEY" "${SSH_USER}@${_ip}" true 2>/dev/null; then
            break
        fi
        if (( _attempt == 30 )); then
            echo "ERROR: cannot SSH to $_ip after 60s" >&2
            exit 1
        fi
        sleep 2
    done
done

remote() {
    local host="$1"; shift
    ssh $SSH_OPTS -i "$SSH_KEY" "${SSH_USER}@${host}" "$@"
}

scp_from() {
    local host="$1" remote_path="$2" local_path="$3"
    scp $SSH_OPTS -i "$SSH_KEY" "${SSH_USER}@${host}:${remote_path}" "$local_path"
}

echo "=== Initializing control plane at $CP_IP ===" >&2

remote "$CP_IP" "sudo kubeadm reset -f 2>/dev/null || true"
remote "$CP_IP" "sudo kubeadm init \
    --apiserver-advertise-address=$CP_IP \
    --pod-network-cidr=$POD_CIDR 2>&1" \
    | sed -E 's/[a-z0-9]{6}\.[a-z0-9]{16}/***TOKEN***/g; s/(sha256:)[a-f0-9]{64}/\1***REDACTED***/g' >&2

# Set up kubectl on the control plane
remote "$CP_IP" "mkdir -p ~/.kube && sudo cp /etc/kubernetes/admin.conf ~/.kube/config && sudo chown \$(id -u):\$(id -g) ~/.kube/config"

# Install Flannel CNI
remote "$CP_IP" "kubectl apply -f https://github.com/flannel-io/flannel/releases/download/v0.26.3/kube-flannel.yml 2>&1" >&2
# Wait for Flannel to write its CNI config before restarting CoreDNS
echo "=== Waiting for Flannel CNI config on control plane ===" >&2
for (( attempt=0; attempt<30; attempt++ )); do
    if remote "$CP_IP" "test -f /etc/cni/net.d/10-flannel.conflist" 2>/dev/null; then
        break
    fi
    sleep 2
done
remote "$CP_IP" "kubectl rollout restart deployment/coredns -n kube-system 2>&1" >&2

# Get join command
JOIN_CMD=$(remote "$CP_IP" "kubeadm token create --print-join-command 2>/dev/null")
echo "Join command obtained." >&2

# Join workers
IFS=',' read -ra WORKER_IPS <<< "$WORKERS"
for worker_ip in "${WORKER_IPS[@]}"; do
    echo "=== Joining worker $worker_ip ===" >&2
    remote "$worker_ip" "sudo $JOIN_CMD 2>&1"
done

# Side-load container image if provided
if [[ -n "$IMAGE_TAR" ]]; then
    echo "=== Importing container image to all nodes ===" >&2
    ALL_NODES=("$CP_IP" "${WORKER_IPS[@]}")
    for node_ip in "${ALL_NODES[@]}"; do
        echo "  Importing to $node_ip..." >&2
        scp $SSH_OPTS -i "$SSH_KEY" "$IMAGE_TAR" "${SSH_USER}@${node_ip}:/tmp/spur-image.tar"
        remote "$node_ip" "sudo ctr -n k8s.io images import /tmp/spur-image.tar && rm -f /tmp/spur-image.tar"
    done
fi

# Wait for all nodes to be Ready
echo "=== Waiting for nodes to be Ready ===" >&2
for (( attempt=0; attempt<60; attempt++ )); do
    not_ready=$(remote "$CP_IP" "kubectl get nodes --no-headers 2>/dev/null | grep -v ' Ready ' | wc -l" || echo "99")
    if [[ "$not_ready" == "0" ]]; then
        echo "All nodes Ready." >&2
        break
    fi
    sleep 5
done

remote "$CP_IP" "kubectl get nodes -o wide" >&2

# Label worker nodes so the spur-k8s-operator registers them as compute nodes
echo "=== Labeling worker nodes with spur.amd.com/managed=true ===" >&2
NODE_LIST=$(remote "$CP_IP" "kubectl get nodes -o wide --no-headers")
for worker_ip in "${WORKER_IPS[@]}"; do
    worker_name=$(echo "$NODE_LIST" | awk -v ip="$worker_ip" '$6 == ip {print $1}')
    if [[ -n "$worker_name" ]]; then
        remote "$CP_IP" "kubectl label node $worker_name spur.amd.com/managed=true --overwrite" >&2
    fi
done

# Install local-path-provisioner for PVC support
echo "=== Installing local-path-provisioner ===" >&2
remote "$CP_IP" "kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.30/deploy/local-path-storage.yaml 2>&1" >&2
remote "$CP_IP" "kubectl patch storageclass local-path -p '{\"metadata\":{\"annotations\":{\"storageclass.kubernetes.io/is-default-class\":\"true\"}}}' 2>&1" >&2

# Install AMD GPU device plugin
echo "=== Installing AMD GPU device plugin ===" >&2
remote "$CP_IP" "kubectl apply -f https://raw.githubusercontent.com/ROCm/k8s-device-plugin/v1.25.2.7/k8s-ds-amdgpu-dp.yaml 2>&1" >&2 || \
    echo "WARNING: AMD GPU device plugin install failed (may not be available)" >&2

# Fetch kubeconfig
scp_from "$CP_IP" ".kube/config" "$KUBECONFIG_OUT"
# Replace the internal API server address with the control plane IP
sed -i "s|server: https://.*:6443|server: https://${CP_IP}:6443|" "$KUBECONFIG_OUT"

echo "KUBECONFIG=$KUBECONFIG_OUT"
echo "K8s cluster ready: 1 control-plane + ${#WORKER_IPS[@]} workers" >&2
