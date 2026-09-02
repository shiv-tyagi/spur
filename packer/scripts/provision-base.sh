#!/usr/bin/env bash
# Provisioning payload for the spur-ci-base image.
# Called by Packer inside the build VM. ROCM_VERSION is passed via environment.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

ROCM_VERSION="${ROCM_VERSION:-6.4}"

echo "=== Installing base packages ==="
sudo apt-get update -qq
sudo apt-get install -y -qq \
    openssh-server \
    python3 python3-pip python3-venv \
    curl wget git jq \
    ca-certificates \
    util-linux \
    iproute2 iputils-ping \
    wireguard-tools \
    squashfs-tools \
    crun podman

echo "=== Enabling WireGuard kernel module (in-tree on noble) ==="
echo wireguard | sudo tee /etc/modules-load.d/wireguard.conf > /dev/null

echo "=== Installing Docker CE ==="
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu noble stable" | \
    sudo tee /etc/apt/sources.list.d/docker.list
sudo apt-get update -qq
sudo apt-get install -y -qq docker-ce docker-ce-cli containerd.io
sudo usermod -aG docker ci

echo "=== Registering AMD repositories ==="
sudo mkdir -p /etc/apt/keyrings
wget -q -O - https://repo.radeon.com/rocm/rocm.gpg.key | gpg --dearmor | sudo tee /etc/apt/keyrings/rocm.gpg > /dev/null
echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/rocm.gpg] https://repo.radeon.com/rocm/apt/${ROCM_VERSION} noble main" | \
    sudo tee /etc/apt/sources.list.d/rocm.list
echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/rocm.gpg] https://repo.radeon.com/amdgpu/${ROCM_VERSION}/ubuntu noble main" | \
    sudo tee /etc/apt/sources.list.d/amdgpu.list
printf 'Package: *\nPin: release o=repo.radeon.com\nPin-Priority: 600\n' | \
    sudo tee /etc/apt/preferences.d/rocm-pin-600 > /dev/null
sudo apt-get update -qq

echo "=== Installing amdgpu kernel driver (DKMS) + DRM helper modules ==="
sudo apt-get install -y -qq \
    "linux-modules-extra-$(uname -r)" \
    amdgpu-dkms
echo amdgpu | sudo tee /etc/modules-load.d/amdgpu.conf > /dev/null

echo "=== Installing ROCm userspace ==="
sudo apt-get install -y -qq \
    rocm-hip-runtime-dev \
    rocm-smi-lib \
    amd-smi-lib

sudo usermod -aG render,video ci

echo "=== Setting up GPU venv (PyTorch ROCm) ==="
sudo mkdir -p /opt/spur-ci
sudo chown ci:ci /opt/spur-ci
python3 -m venv /opt/spur-ci/gpu-venv
source /opt/spur-ci/gpu-venv/bin/activate
pip install --quiet "torch==2.9.1+rocm${ROCM_VERSION}" \
    --index-url "https://download.pytorch.org/whl/rocm${ROCM_VERSION}"
pip install --quiet numpy
deactivate

echo "=== Setting up test venv (driver-VM harness deps) ==="
python3 -m venv /opt/spur-ci/test-venv
source /opt/spur-ci/test-venv/bin/activate
pip install --quiet "pytest==8.3.*" "pyyaml==6.0.*" "paramiko==3.5.*" "requests==2.32.*" "kubernetes==31.*" "tomli_w==1.1.*"

echo "=== Base provisioning complete ==="
