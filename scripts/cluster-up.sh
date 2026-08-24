#!/usr/bin/env bash
# Launch an ephemeral multi-VM cluster for Spur CI.
# Each VM gets a COW qcow2 overlay, cloud-init for hostname/SSH, and optional GPU passthrough.
# VMs get IPs via DHCP from libvirt's default network.
#
# Output (eval-able):
#   CLUSTER_VMS="prefix-0,prefix-1,prefix-2"
#   CLUSTER_IPS="192.168.122.x,192.168.122.y,192.168.122.z"
#   PARTITION_LEASE="/var/lib/spur-ci/locks/lease-XXXXX"

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTANCE_DIR="${SPUR_CI_INSTANCE_DIR:-/var/lib/spur-ci/instances}"
IMAGE_DIR="${SPUR_CI_IMAGE_DIR:-/var/lib/spur-ci/images}"

usage() {
    cat <<EOF
Usage: $(basename "$0") [options]

Options:
  --prefix NAME        VM name prefix (required)
  --count N            Number of VMs (default: 3)
  --image PATH         Base qcow2 image path (required)
  --gpus-per-vm SPEC   GPUs per VM via VFIO passthrough (default: 0). Either a
                       scalar (uniform, e.g. 2) or a comma list of length --count
                       (per-VM, e.g. 0,2,1,1). A 0 entry means that VM gets no GPU.
  --skip-gpu-for IDX   Force 0 GPUs for VM at index IDX (repeatable; back-compat,
                       equivalent to a 0 in the --gpus-per-vm list)
  --cpus N             vCPUs per VM (default: 4)
  --memory MiB         RAM per VM in MiB (default: 16384)
  --ssh-pubkey PATH    SSH public key to inject (default: ~/.ssh/id_ed25519.pub)
  --disk-size SIZE     COW overlay disk size (default: 40G)
  --network NAME       Libvirt network name (default: \$SPUR_CI_NETWORK or 'default')
  --gpu-vfs LIST       Comma-separated PCI VF addresses (bypasses gpu-partition.sh discovery)

Output is eval-able shell variables.
EOF
    exit 1
}

PREFIX=""
COUNT=3
IMAGE=""
GPUS_PER_VM=0
CPUS=4
MEMORY=16384
SSH_PUBKEY="${HOME}/.ssh/id_ed25519.pub"
DISK_SIZE="60G"
SKIP_GPU_FOR=()
NETWORK="${SPUR_CI_NETWORK:-default}"
STATIC_GPU_VFS="${SPUR_CI_GPU_VFS:-}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --prefix) PREFIX="$2"; shift 2 ;;
        --count) COUNT="$2"; shift 2 ;;
        --image) IMAGE="$2"; shift 2 ;;
        --gpus-per-vm) GPUS_PER_VM="$2"; shift 2 ;;
        --skip-gpu-for) SKIP_GPU_FOR+=("$2"); shift 2 ;;
        --cpus) CPUS="$2"; shift 2 ;;
        --memory) MEMORY="$2"; shift 2 ;;
        --ssh-pubkey) SSH_PUBKEY="$2"; shift 2 ;;
        --disk-size) DISK_SIZE="$2"; shift 2 ;;
        --network) NETWORK="$2"; shift 2 ;;
        --gpu-vfs) STATIC_GPU_VFS="$2"; shift 2 ;;
        *) echo "Unknown option: $1" >&2; usage ;;
    esac
done

[[ -z "$PREFIX" ]] && { echo "ERROR: --prefix required" >&2; usage; }
[[ -z "$IMAGE" ]] && { echo "ERROR: --image required" >&2; usage; }
[[ ! -f "$IMAGE" ]] && { echo "ERROR: base image not found: $IMAGE" >&2; exit 1; }
[[ ! -f "$SSH_PUBKEY" ]] && { echo "ERROR: SSH pubkey not found: $SSH_PUBKEY" >&2; exit 1; }

SSH_KEY_CONTENT=$(cat "$SSH_PUBKEY")
RUN_DIR="$INSTANCE_DIR/$PREFIX"
mkdir -p "$RUN_DIR"

# Build a per-VM GPU count array from --gpus-per-vm (scalar or comma list).
GPU_COUNTS=()
if [[ "$GPUS_PER_VM" == *,* ]]; then
    IFS=',' read -ra GPU_COUNTS <<< "$GPUS_PER_VM"
    if (( ${#GPU_COUNTS[@]} != COUNT )); then
        echo "ERROR: --gpus-per-vm list has ${#GPU_COUNTS[@]} entries but --count is $COUNT" >&2
        exit 1
    fi
else
    for (( i=0; i<COUNT; i++ )); do GPU_COUNTS+=("$GPUS_PER_VM"); done
fi

# --skip-gpu-for zeroes those VMs (back-compat; subsumed by a 0 in the list).
for skip_idx in "${SKIP_GPU_FOR[@]}"; do
    (( skip_idx >= 0 && skip_idx < COUNT )) && GPU_COUNTS[$skip_idx]=0
done

TOTAL_GPUS=0
for c in "${GPU_COUNTS[@]}"; do
    [[ "$c" =~ ^[0-9]+$ ]] || { echo "ERROR: --gpus-per-vm entries must be non-negative integers, got '$c'" >&2; exit 1; }
    TOTAL_GPUS=$(( TOTAL_GPUS + c ))
done

# Acquire GPUs if needed (libvirt managed mode handles driver bind/unbind)
LEASE_FILE=""
GPU_PCI_ARRAY=()
if (( TOTAL_GPUS > 0 )); then
    if [[ -n "$STATIC_GPU_VFS" ]]; then
        IFS=',' read -ra GPU_PCI_ARRAY <<< "$STATIC_GPU_VFS"
        if (( ${#GPU_PCI_ARRAY[@]} < TOTAL_GPUS )); then
            echo "ERROR: --gpu-vfs provides ${#GPU_PCI_ARRAY[@]} VFs but need $TOTAL_GPUS" >&2
            exit 1
        fi
        echo "Using static VF list: ${STATIC_GPU_VFS}" >&2
    else
        eval "$("$SCRIPT_DIR/gpu-partition.sh" acquire --count "$TOTAL_GPUS")"
        IFS=',' read -ra GPU_PCI_ARRAY <<< "$GPU_PCI_ADDRS"
        echo "Acquired ${#GPU_PCI_ARRAY[@]} GPUs via lease: ${GPU_PCI_ADDRS}" >&2
    fi
fi


gpu_idx=0
VM_NAMES=()

for (( i=0; i<COUNT; i++ )); do
    vm_name="${PREFIX}-${i}"
    overlay="$RUN_DIR/${vm_name}.qcow2"
    ci_iso="$RUN_DIR/${vm_name}-cidata.iso"

    VM_NAMES+=("$vm_name")

    echo "Creating VM $vm_name..." >&2

    # Create COW overlay
    qemu-img create -f qcow2 -b "$IMAGE" -F qcow2 "$overlay" "$DISK_SIZE" >/dev/null

    # Generate cloud-init (DHCP — no static IP config needed)
    ci_dir="$RUN_DIR/${vm_name}-ci"
    mkdir -p "$ci_dir"

    cat > "$ci_dir/meta-data" <<METAEOF
instance-id: $vm_name
local-hostname: $vm_name
METAEOF

    cat > "$ci_dir/user-data" <<UDEOF
#cloud-config
hostname: $vm_name
manage_etc_hosts: true
users:
  - name: ci
    sudo: ALL=(ALL) NOPASSWD:ALL
    shell: /bin/bash
    ssh_authorized_keys:
      - $SSH_KEY_CONTENT
ssh_pwauth: false
package_update: false
package_upgrade: false
runcmd:
  - systemctl enable --now ssh || systemctl enable --now sshd
UDEOF

    # Empty network-config: let cloud-init fall back to DHCP using MAC address
    touch "$ci_dir/network-config"

    genisoimage -output "$ci_iso" -volid cidata -joliet -rock \
        "$ci_dir/meta-data" "$ci_dir/user-data" "$ci_dir/network-config" 2>/dev/null

    # Build virt-install command
    virt_args=(
        virt-install
        --name "$vm_name"
        --ram "$MEMORY"
        --vcpus "$CPUS"
        --disk "path=$overlay,format=qcow2,bus=virtio"
        --disk "path=$ci_iso,device=cdrom"
        --os-variant ubuntu24.04
        --network network="${NETWORK}",model=virtio
        --graphics none
        --console pty,target_type=serial
        --noautoconsole
        --import
        --cpu host-passthrough
        --sysinfo system.serial=ds=nocloud
    )

    # Add GPU passthrough devices
    n_gpus="${GPU_COUNTS[$i]}"
    for (( g=0; g<n_gpus; g++ )); do
        if (( gpu_idx < ${#GPU_PCI_ARRAY[@]} )); then
            pci="${GPU_PCI_ARRAY[$gpu_idx]}"
            virt_args+=(--hostdev "$pci,type=pci")
            gpu_idx=$((gpu_idx + 1))
        fi
    done

    sudo "${virt_args[@]}" >/dev/null

done

# Discover VM IPs via virsh domifaddr (DHCP-assigned) — poll all VMs together
echo "Waiting for VMs to boot and get IPs..." >&2
declare -A VM_IP_MAP
for (( attempt=0; attempt<90; attempt++ )); do
    all_found=true
    for (( i=0; i<COUNT; i++ )); do
        vm_name="${VM_NAMES[$i]}"
        if [[ -z "${VM_IP_MAP[$vm_name]:-}" ]]; then
            vm_ip=$(sudo virsh domifaddr "$vm_name" 2>/dev/null | grep -oP '\d+\.\d+\.\d+\.\d+' | head -1) || true
            if [[ -n "$vm_ip" ]]; then
                VM_IP_MAP[$vm_name]="$vm_ip"
                echo "  $vm_name -> $vm_ip" >&2
            else
                all_found=false
            fi
        fi
    done
    if $all_found; then
        break
    fi
    sleep 2
done

VM_IPS=()
for (( i=0; i<COUNT; i++ )); do
    vm_name="${VM_NAMES[$i]}"
    if [[ -z "${VM_IP_MAP[$vm_name]:-}" ]]; then
        echo "ERROR: VM $vm_name did not get an IP within 180s" >&2
        "$SCRIPT_DIR/cluster-down.sh" --prefix "$PREFIX" --count "$COUNT" 2>/dev/null || true
        [[ -n "$LEASE_FILE" ]] && "$SCRIPT_DIR/gpu-partition.sh" release --lease "$LEASE_FILE" 2>/dev/null || true
        exit 1
    fi
    VM_IPS+=("${VM_IP_MAP[$vm_name]}")
done

# Wait for SSH to be reachable on all VMs
echo "Waiting for SSH..." >&2
for (( attempt=0; attempt<45; attempt++ )); do
    all_up=true
    for ip in "${VM_IPS[@]}"; do
        SSH_PRIVKEY="${SSH_PUBKEY%.pub}"
        if ! ssh -o ConnectTimeout=2 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
             -o BatchMode=yes -i "$SSH_PRIVKEY" ci@"$ip" true 2>/dev/null; then
            all_up=false
            break
        fi
    done
    if $all_up; then
        break
    fi
    sleep 2
done

if ! $all_up; then
    echo "ERROR: not all VMs became SSH-reachable within 90s" >&2
    "$SCRIPT_DIR/cluster-down.sh" --prefix "$PREFIX" --count "$COUNT" 2>/dev/null || true
    [[ -n "$LEASE_FILE" ]] && "$SCRIPT_DIR/gpu-partition.sh" release --lease "$LEASE_FILE" 2>/dev/null || true
    exit 1
fi

echo "All $COUNT VMs ready." >&2

# Output eval-able variables
printf 'CLUSTER_VMS="%s"\n' "$(IFS=,; echo "${VM_NAMES[*]}")"
printf 'CLUSTER_IPS="%s"\n' "$(IFS=,; echo "${VM_IPS[*]}")"
printf 'PARTITION_LEASE="%s"\n' "$LEASE_FILE"
for (( idx=0; idx<${#VM_IPS[@]}; idx++ )); do
    printf 'VM_%d_IP="%s"\n' "$idx" "${VM_IPS[$idx]}"
    printf 'VM_%d_NAME="%s"\n' "$idx" "${VM_NAMES[$idx]}"
done
