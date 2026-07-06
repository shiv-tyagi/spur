#!/usr/bin/env bash
# Destroy an ephemeral VM cluster created by cluster-up.sh.
# Idempotent: silently skips VMs that don't exist.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTANCE_DIR="${SPUR_CI_INSTANCE_DIR:-/var/lib/spur-ci/instances}"

usage() {
    cat <<EOF
Usage: $(basename "$0") [options]

Options:
  --prefix NAME    VM name prefix (required)
  --count N        Number of VMs (default: 3)
  --lease FILE     GPU lease file to release (optional)
EOF
    exit 1
}

PREFIX=""
COUNT=3
LEASE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --prefix) PREFIX="$2"; shift 2 ;;
        --count) COUNT="$2"; shift 2 ;;
        --lease) LEASE="$2"; shift 2 ;;
        *) echo "Unknown option: $1" >&2; usage ;;
    esac
done

[[ -z "$PREFIX" ]] && { echo "ERROR: --prefix required" >&2; usage; }

for (( i=0; i<COUNT; i++ )); do
    vm_name="${PREFIX}-${i}"

    if sudo virsh dominfo "$vm_name" &>/dev/null; then
        echo "Destroying VM: $vm_name" >&2
        sudo virsh destroy "$vm_name" 2>/dev/null || true
        sudo virsh undefine "$vm_name" --nvram 2>/dev/null || \
            sudo virsh undefine "$vm_name" 2>/dev/null || true
    else
        echo "VM not found (skipping): $vm_name" >&2
    fi
done

# Clean up instance directory
RUN_DIR="$INSTANCE_DIR/$PREFIX"
if [[ -d "$RUN_DIR" ]]; then
    echo "Removing instance dir: $RUN_DIR" >&2
    rm -rf "$RUN_DIR"
fi

# Release GPU lease (libvirt managed mode handles driver rebinding automatically)
if [[ -n "$LEASE" && -f "$LEASE" ]]; then
    "$SCRIPT_DIR/gpu-partition.sh" release --lease "$LEASE"
fi

echo "Cluster $PREFIX destroyed." >&2
