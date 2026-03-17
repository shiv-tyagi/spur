# Spur Implementation Roadmap

Based on gap analysis of Slurm API surface vs current Spur implementation.
176 tests passing, end-to-end job dispatch working on MI300X cluster.

## Phase 6: Quick Wins (P0, S/M complexity)

Low-effort high-value items that unblock real usage.

| # | Item | Size | Description |
|---|------|------|-------------|
| 6.1 | scontrol update/hold/release | S | Wire stubs to ClusterManager |
| 6.2 | Node stale heartbeat detection | S | Auto-drain nodes on heartbeat timeout |
| 6.3 | Job completion → accounting | S | Wire agent completion to spurdbd |
| 6.4 | FFI error handling | S | slurm_strerror, slurm_get_errno |
| 6.5 | Job dependency enforcement | M | Check afterok/afterany/afternotok/singleton before scheduling |
| 6.6 | GRES wired into scheduler | M | Parse GRES requests, match against node GPUs |
| 6.7 | Node feature constraints | M | --constraint/-C flag parsing + matching |
| 6.8 | Prologue/epilogue scripts | M | Pre/post-job hook execution in spurd |

## Phase 7: Job Arrays + Auth (P0, L complexity)

| # | Item | Size | Description |
|---|------|------|-------------|
| 7.1 | Job array expansion | L | Expand array spec into tasks, schedule independently, track |
| 7.2 | Authentication middleware | L | JWT verification on gRPC + REST, token generation |
| 7.3 | Authorization / ACLs | M | User can only cancel own jobs, admin roles, partition access |
| 7.4 | Preemption execution | L | Cancel/requeue running jobs when higher-priority arrives |
| 7.5 | salloc (interactive alloc) | L | Allocate nodes, hold until user releases |

## Phase 8: Accounting Management (P0, XL)

| # | Item | Size | Description |
|---|------|------|-------------|
| 8.1 | sacctmgr CLI | XL | CRUD for accounts, users, QOS, associations |
| 8.2 | QOS enforcement | XL | Per-QOS limits, priority, preempt policy |
| 8.3 | TRES tracking | M | CPU/mem/GPU/energy as distinct trackable resources |
| 8.4 | Association tree | L | Hierarchical user→account→cluster with per-level limits |

## Phase 9: Multi-Node Parallel (P0, XL)

| # | Item | Size | Description |
|---|------|------|-------------|
| 9.1 | srun interactive execution | XL | Blocking submit, I/O forwarding via gRPC stream |
| 9.2 | Job steps | XL | srun-within-sbatch, step-level tracking |
| 9.3 | Consumable TRES scheduling | XL | Core/socket/GPU granularity, select/cons_tres |
| 9.4 | CPU/GPU binding | L | NUMA affinity, --cpu-bind, --gpu-bind |
| 9.5 | MPI/PMIx launch | XL | PMI key-value server, rank assignment |

## Phase 10: Production Hardening

| # | Item | Size | Description |
|---|------|------|-------------|
| 10.1 | HA controller failover | XL | openraft for bare-metal, K8s Lease |
| 10.2 | Reservations | L | Admin time+node reservations |
| 10.3 | REST API expansion | L | ~30 more endpoints for full slurmrestd compat |
| 10.4 | sdiag/sprio/sshare | S | Scheduler diagnostics, priority breakdown, fair-share tree |
| 10.5 | Cloud/elastic scaling | XL | Suspend/resume programs, cloud node provisioning |
| 10.6 | Container support | L | OCI/Singularity/Enroot integration |
| 10.7 | slurm.conf parser | M | Key=value format for migration from Slurm |
