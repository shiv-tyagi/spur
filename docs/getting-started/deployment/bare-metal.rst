Bare-Metal Deployment
=====================

Deploy Spur across physical or virtual machines connected by a WireGuard mesh.

Install
-------

Get Spur binaries onto every machine (controller + compute nodes).

**Option A — pre-built binaries:**

.. code-block:: bash

   curl -fsSL https://raw.githubusercontent.com/ROCm/spur/main/install.sh | bash
   export PATH="$HOME/.spur/bin:$PATH"

**Option B — build from source:**

.. code-block:: bash

   # Prerequisites: Rust 1.75+ (https://rustup.rs), protobuf-compiler
   sudo apt install protobuf-compiler
   cargo build --release
   # Binaries in target/release/: spur, spurctld, spurd, spurdbd, spurrestd

**Option C — portable binaries** (glibc 2.28+, works on Ubuntu 20.04+, RHEL 8+):

.. code-block:: bash

   ./deploy/build-portable.sh    # outputs to dist/bin/

Network Layout
--------------

.. code-block:: text

   controller (192.168.1.100)  →  WireGuard 10.44.0.1
   gpu-node-1 (192.168.1.101)  →  WireGuard 10.44.0.2
   gpu-node-2 (192.168.1.102)  →  WireGuard 10.44.0.3

WireGuard Mesh
--------------

**On the controller:**

.. code-block:: bash

   sudo spur net init --cidr 10.44.0.0/16 --port 51820

This prints the server public key and a join command template.

**On each compute node:**

.. code-block:: bash

   sudo spur net join \
       --endpoint 192.168.1.100:51820 \
       --server-key <controller-pubkey> \
       --address 10.44.0.2

   # Register this node as a peer on the controller:
   sudo spur net add-peer \
       --key <node-pubkey> \
       --allowed-ip 10.44.0.2/32 \
       --endpoint 192.168.1.101:51820

Repeat for each node, incrementing the address.

**Verify connectivity:**

.. code-block:: bash

   spur net status           # Shows WireGuard peers and handshake times
   ping 10.44.0.1            # Ping the controller through the mesh

Configuration
-------------

Create ``/etc/spur/spur.conf`` on all nodes:

.. code-block:: toml

   cluster_name = "gpu-cluster"

   [controller]
   listen_addr = "[::]:6817"
   hosts = ["10.44.0.1"]
   state_dir = "/var/spool/spur"

   [scheduler]
   plugin = "backfill"
   interval_secs = 1

   [network]
   wg_enabled = true
   wg_interface = "spur0"
   agent_port = 6818

   [[partitions]]
   name = "gpu"
   default = true
   nodes = "gpu-node-[1-2]"
   max_time = "72:00:00"

   [[nodes]]
   names = "gpu-node-[1-2]"
   cpus = 128
   memory_mb = 512000
   gres = ["gpu:mi300x:8"]

Start the Daemons
-----------------

**Controller (10.44.0.1):**

.. code-block:: bash

   sudo mkdir -p /var/spool/spur
   spurctld -D -f /etc/spur/spur.conf

**Each compute node:**

.. code-block:: bash

   spurd -D \
       --controller http://10.44.0.1:6817 \
       --hostname gpu-node-1 \
       --listen [::]:6818

The agent detects the ``spur0`` WireGuard interface and registers with its mesh address.

Submitting Jobs
---------------

.. code-block:: bash

   cat > train.sh << 'EOF'
   #!/bin/bash
   #SBATCH --job-name=distributed-training
   #SBATCH -N 2
   #SBATCH --ntasks-per-node=8
   #SBATCH --gres=gpu:mi300x:8
   #SBATCH --time=4:00:00

   torchrun \
       --nnodes=$SPUR_NUM_NODES \
       --node_rank=$SPUR_TASK_OFFSET \
       --master_addr=$(echo $SPUR_PEER_NODES | cut -d: -f1) \
       --master_port=29500 \
       --nproc_per_node=8 \
       train.py
   EOF

   spur submit train.sh

Environment Variables
---------------------

Each node in a multi-node job receives:

.. list-table::
   :header-rows: 1

   * - Variable
     - Example
     - Description
   * - ``SPUR_JOB_ID``
     - ``42``
     - Job ID
   * - ``SPUR_NUM_NODES``
     - ``2``
     - Total nodes in allocation
   * - ``SPUR_TASK_OFFSET``
     - ``0`` or ``8``
     - This node's starting task index
   * - ``SPUR_PEER_NODES``
     - ``10.44.0.2:6818,10.44.0.3:6818``
     - All nodes in the allocation
   * - ``SPUR_CPUS_ON_NODE``
     - ``128``
     - CPUs allocated on this node

GPU Isolation
-------------

Spur automatically restricts GPU visibility per job:

- **AMD (ROCm):** Sets ``ROCR_VISIBLE_DEVICES``
- **NVIDIA (CUDA):** Sets ``CUDA_VISIBLE_DEVICES``
