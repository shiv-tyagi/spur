#!/usr/bin/env bash
# Provisioning payload for the spur-ci-k8s image (layered on spur-ci-base).
# Called by Packer inside the build VM. K8S_VERSION is passed via environment.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

K8S_VERSION="${K8S_VERSION:-1.36}"

echo "=== Installing containerd and dependencies ==="
sudo apt-get update -qq
sudo apt-get install -y -qq containerd conntrack ebtables socat

sudo mkdir -p /etc/containerd
containerd config default | sudo tee /etc/containerd/config.toml > /dev/null
sudo sed -i 's/SystemdCgroup = false/SystemdCgroup = true/' /etc/containerd/config.toml
sudo systemctl enable containerd

echo "=== Configuring kernel modules and sysctl for Kubernetes ==="
cat <<EOF | sudo tee /etc/modules-load.d/k8s.conf
overlay
br_netfilter
EOF
sudo modprobe overlay
sudo modprobe br_netfilter

cat <<EOF | sudo tee /etc/sysctl.d/k8s.conf
net.bridge.bridge-nf-call-iptables  = 1
net.bridge.bridge-nf-call-ip6tables = 1
net.ipv4.ip_forward                 = 1
EOF
sudo sysctl --system > /dev/null 2>&1

echo "=== Installing kubeadm, kubelet, kubectl ${K8S_VERSION} ==="
sudo mkdir -p /etc/apt/keyrings
curl -fsSL "https://pkgs.k8s.io/core:/stable:/v${K8S_VERSION}/deb/Release.key" | \
    sudo gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
echo "deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v${K8S_VERSION}/deb/ /" | \
    sudo tee /etc/apt/sources.list.d/kubernetes.list

sudo apt-get update -qq
sudo apt-get install -y -qq "kubelet=${K8S_VERSION}.*" "kubeadm=${K8S_VERSION}.*" "kubectl=${K8S_VERSION}.*"
sudo apt-mark hold kubelet kubeadm kubectl

echo "=== Pre-pulling Kubernetes images ==="
sudo kubeadm config images pull --kubernetes-version "$(kubeadm version -o short)" 2>/dev/null || \
    echo "WARNING: image pull had issues (non-fatal during build)"

echo "=== Removing default CNI configs (Flannel must be sole CNI) ==="
sudo rm -f /etc/cni/net.d/10-containerd-net.conflist \
           /etc/cni/net.d/87-podman-bridge.conflist \
           /etc/cni/net.d/100-crio-bridge.conflist
# Leave only .kubernetes-cni-keep so the directory persists

echo "=== Disabling swap ==="
sudo swapoff -a || true
sudo sed -i '/\sswap\s/d' /etc/fstab

echo "=== K8s provisioning complete ==="
