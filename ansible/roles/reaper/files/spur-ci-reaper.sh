#!/usr/bin/env bash
# Reap orphaned CI VMs older than MAX_AGE_MINUTES.
# Only reaps VMs whose GITHUB_RUN_ID (embedded in the VM prefix) does not match
# any currently running runner process.

set -euo pipefail

MAX_AGE_MINUTES="${SPUR_CI_REAPER_MAX_AGE:-60}"
INSTANCE_DIR="${SPUR_CI_MOUNT:-/var/lib/spur-ci}/instances"
LOCK_DIR="${SPUR_CI_MOUNT:-/var/lib/spur-ci}/locks"

echo "[reaper] Scanning for orphaned VMs older than ${MAX_AGE_MINUTES}m..."

active_run_ids=()
while IFS= read -r pid; do
    run_id=$(tr '\0' '\n' < /proc/"$pid"/environ 2>/dev/null | grep -oP '^GITHUB_RUN_ID=\K.*' || true)
    if [[ -n "$run_id" ]]; then
        active_run_ids+=("$run_id")
    fi
done < <(pgrep -f 'Runner.Worker' 2>/dev/null || true)

# List all running VMs with "ci-" prefix
virsh list --name 2>/dev/null | grep '^ci-' | while read -r vm_name; do
    # Extract the run ID from the VM name (format: ci-<host>-<slot>-<run_id>-<idx>)
    vm_run_id=$(echo "$vm_name" | grep -oP '\d{9,}' || true)

    # Skip if this run ID is still active
    if [[ -n "$vm_run_id" ]]; then
        for active_id in "${active_run_ids[@]}"; do
            if [[ "$vm_run_id" == "$active_id" ]]; then
                continue 2
            fi
        done
    fi

    # Check age via qemu process
    pid=$(pgrep -f "guest=$vm_name" | head -1 2>/dev/null) || true
    if [[ -n "$pid" && -d "/proc/$pid" ]]; then
        age_seconds=$(( $(date +%s) - $(stat -c %Y /proc/"$pid") ))
        age_minutes=$(( age_seconds / 60 ))

        if (( age_minutes > MAX_AGE_MINUTES )); then
            echo "[reaper] Destroying orphaned VM: $vm_name (age: ${age_minutes}m, run_id: ${vm_run_id:-unknown})"
            virsh destroy "$vm_name" 2>/dev/null || true
            virsh undefine "$vm_name" --nvram 2>/dev/null || true
        fi
    fi
done

# Clean up orphaned overlay files
if [[ -d "$INSTANCE_DIR" ]]; then
    find "$INSTANCE_DIR" -name "*.qcow2" -mmin "+${MAX_AGE_MINUTES}" -type f | while read -r overlay; do
        dir_name=$(basename "$(dirname "$overlay")")
        if ! virsh dominfo "$dir_name" &>/dev/null; then
            echo "[reaper] Removing orphaned overlay: $overlay"
            rm -f "$overlay"
        fi
    done
fi

# Clean up stale lease files
if [[ -d "$LOCK_DIR" ]]; then
    find "$LOCK_DIR" -name "lease-*" -mmin "+${MAX_AGE_MINUTES}" -type f -delete
fi

echo "[reaper] Done."
