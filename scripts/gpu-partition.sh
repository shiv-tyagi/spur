#!/usr/bin/env bash
# GPU VF lease manager for CI VM clusters.
# Auto-discovers SR-IOV Virtual Functions via lspci.
# Uses flock-based leasing to prevent concurrent CI jobs from claiming the same VFs.
#
# NOTE: The flock-based leasing has a known limitation: lock FDs are released when this
# script exits (since acquire() is called in a subshell via eval). Static per-slot VF
# pinning (SPUR_CI_GPU_VFS) is the primary mechanism for slot isolation; flock leasing
# is kept as a best-effort safety net for ad-hoc usage.

set -euo pipefail

LOCK_DIR="${SPUR_CI_LOCK_DIR:-/var/lib/spur-ci/locks}"

usage() {
    cat <<EOF
Usage: $(basename "$0") <command> [options]

Commands:
  acquire --count N    Lease N GPU VFs. Prints PCI addresses to stdout.
  slot --count N       Select N VFs from SPUR_CI_GPU_VFS (static slot pinning, no lease).
  release --lease FILE Release a previously acquired lease.
  status               Show which VFs are free/leased.

Environment:
  SPUR_CI_LOCK_DIR     Lock directory (default: /var/lib/spur-ci/locks)
  SPUR_CI_GPU_VFS      Comma-separated VF PCI addresses for slot mode
EOF
    exit 1
}

read_gpu_list() {
    if [[ -n "${SPUR_CI_GPU_VFS:-}" ]]; then
        tr ',' '\n' <<< "$SPUR_CI_GPU_VFS"
    else
        lspci -D | grep -i 'VF\|Virtual Function' | grep -i 'amd\|radeon' | awk '{print $1}'
    fi
}

read_gpu_count() {
    read_gpu_list | wc -l
}

acquire() {
    local count="$1"
    local total
    total=$(read_gpu_count)

    if (( count > total )); then
        echo "ERROR: requested $count GPUs but only $total available" >&2
        exit 1
    fi

    mkdir -p "$LOCK_DIR"

    local lease_file
    lease_file=$(mktemp "$LOCK_DIR/lease-XXXXXX")

    local acquired=0
    local pci_list=()
    local lock_fds=()

    while IFS= read -r pci; do
        local lock_file="$LOCK_DIR/gpu-${pci//[:.]/-}.lock"

        exec {fd}>"$lock_file"
        if flock -n "$fd"; then
            pci_list+=("$pci")
            lock_fds+=("$fd")
            acquired=$((acquired + 1))
            if (( acquired >= count )); then
                break
            fi
        else
            exec {fd}>&-
        fi
    done < <(read_gpu_list)

    if (( acquired < count )); then
        for fd in "${lock_fds[@]}"; do
            exec {fd}>&-
        done
        rm -f "$lease_file"
        echo "ERROR: could only acquire $acquired of $count GPUs" >&2
        exit 1
    fi

    # Write lease metadata — the lock FDs stay open in the caller's process tree
    {
        echo "lease_file=$lease_file"
        echo "gpu_count=$count"
        echo "created=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo "pid=$$"
        for i in "${!pci_list[@]}"; do
            echo "gpu_${i}_pci=${pci_list[$i]}"
            echo "gpu_${i}_lock=$LOCK_DIR/gpu-${pci_list[$i]//[:.]/-}.lock"
        done
    } > "$lease_file"

    echo "LEASE_FILE=$lease_file"
    printf "GPU_PCI_ADDRS=%s\n" "$(IFS=,; echo "${pci_list[*]}")"
}

release() {
    local lease_file="$1"

    if [[ ! -f "$lease_file" ]]; then
        echo "WARNING: lease file $lease_file not found, nothing to release" >&2
        return 0
    fi

    local lock_files
    lock_files=$(grep '_lock=' "$lease_file" | cut -d= -f2-)

    while IFS= read -r lock_file; do
        rm -f "$lock_file"
    done <<< "$lock_files"

    rm -f "$lease_file"
    echo "Released lease: $lease_file"
}

status() {
    echo "GPU Partition Status:"
    echo "====================="
    while IFS= read -r pci; do
        local lock_file="$LOCK_DIR/gpu-${pci//[:.]/-}.lock"
        if [[ -f "$lock_file" ]] && ! flock -n "$lock_file" true 2>/dev/null; then
            echo "  $pci: LEASED"
        else
            echo "  $pci: FREE"
        fi
    done < <(read_gpu_list)
}

[[ $# -lt 1 ]] && usage

cmd="$1"; shift

case "$cmd" in
    acquire)
        count=""
        while [[ $# -gt 0 ]]; do
            case "$1" in
                --count) count="$2"; shift 2 ;;
                *) usage ;;
            esac
        done
        [[ -z "$count" ]] && usage
        acquire "$count"
        ;;
    slot)
        count=""
        while [[ $# -gt 0 ]]; do
            case "$1" in
                --count) count="$2"; shift 2 ;;
                *) usage ;;
            esac
        done
        [[ -z "$count" ]] && usage
        [[ -z "${SPUR_CI_GPU_VFS:-}" ]] && { echo "ERROR: SPUR_CI_GPU_VFS not set" >&2; exit 1; }
        IFS=',' read -ra slot_vfs <<< "$SPUR_CI_GPU_VFS"
        if (( ${#slot_vfs[@]} < count )); then
            echo "ERROR: slot has ${#slot_vfs[@]} VFs but need $count" >&2
            exit 1
        fi
        selected=("${slot_vfs[@]:0:$count}")
        printf "GPU_PCI_ADDRS=%s\n" "$(IFS=,; echo "${selected[*]}")"
        ;;
    release)
        lease=""
        while [[ $# -gt 0 ]]; do
            case "$1" in
                --lease) lease="$2"; shift 2 ;;
                *) usage ;;
            esac
        done
        [[ -z "$lease" ]] && usage
        release "$lease"
        ;;
    status)
        status
        ;;
    *)
        usage
        ;;
esac
