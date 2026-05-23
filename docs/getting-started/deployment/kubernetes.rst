Kubernetes Deployment
=====================

Deploy Spur on an existing Kubernetes cluster. The controller runs as a StatefulSet with Raft consensus, and compute nodes are managed by the ``spur-k8s-operator``.

Prerequisites
-------------

- Kubernetes 1.28+
- ``kubectl`` configured with cluster-admin access

Build and load container images:

.. code-block:: bash

   # Build
   docker build -f deploy/Dockerfile -t spur:<tag> .

   # Load onto each node (if not using a registry)
   docker save spur:<tag> -o spur.tar
   # SCP to each node, then:
   sudo ctr -n k8s.io images import spur.tar

Architecture
------------

.. code-block:: text

   spur namespace
   ├── spurctld     StatefulSet (3 replicas, Raft consensus)
   ├── spurd        DaemonSet or Deployment (compute agents)
   ├── spurdbd      Deployment (accounting, PostgreSQL)
   ├── spurrestd    Deployment (REST API)
   └── operator     Deployment (watches SpurJob CRDs)

All manifests live in ``deploy/k8s/``.

Deploy
------

Apply manifests in order:

.. code-block:: bash

   kubectl apply -f deploy/k8s/namespace.yaml
   kubectl apply -f deploy/k8s/configmap.yaml
   kubectl apply -f deploy/k8s/rbac.yaml
   kubectl apply -f deploy/k8s/spurctld.yaml
   kubectl apply -f deploy/k8s/spurd.yaml
   kubectl apply -f deploy/k8s/spurdbd.yaml
   kubectl apply -f deploy/k8s/spurrestd.yaml
   kubectl apply -f deploy/k8s/operator.yaml
   kubectl apply -f deploy/k8s/pdb.yaml

Configuration
-------------

The ConfigMap (``deploy/k8s/configmap.yaml``) embeds ``spur.conf``:

.. code-block:: toml

   cluster_name = "spur-k8s"

   [controller]
   peers = [
     "spurctld-0.spurctld.spur.svc.cluster.local:6821",
     "spurctld-1.spurctld.spur.svc.cluster.local:6821",
     "spurctld-2.spurctld.spur.svc.cluster.local:6821",
   ]

   [scheduler]
   interval_secs = 2
   plugin = "backfill"

   [[partitions]]
   name = "default"
   state = "UP"
   default = true

Raft peers use StatefulSet DNS names. The node ID is auto-detected from the pod hostname ordinal (``spurctld-N`` → node ID ``N+1``).

Adjust partition definitions and node resources to match your cluster hardware.

Submitting Jobs
---------------

Jobs are submitted as ``SpurJob`` custom resources:

.. code-block:: yaml

   apiVersion: spur.io/v1
   kind: SpurJob
   metadata:
     name: training-run
   spec:
     script: |
       #!/bin/bash
       #SBATCH --job-name=train
       #SBATCH -N 2
       #SBATCH --gres=gpu:8
       torchrun --nnodes=2 train.py

Apply with ``kubectl``:

.. code-block:: bash

   kubectl apply -f job.yaml

The operator watches SpurJob resources, submits them to the controller, and updates status fields as the job progresses.

Verify
------

.. code-block:: bash

   # All pods running
   kubectl get pods -n spur

   # Controller logs (check Raft leader election)
   kubectl logs statefulset/spurctld -n spur

   # Node registration
   kubectl exec -n spur spurctld-0 -- spur nodes

Scaling
-------

**Controller HA:** The StatefulSet runs 3 replicas by default. Raft automatically elects a leader. Scale to 5 for larger clusters.

**Compute agents:** Add worker nodes to the cluster; if ``spurd`` is a DaemonSet it will schedule automatically. Otherwise scale the Deployment replica count.

**Disruption budgets:** ``deploy/k8s/pdb.yaml`` ensures at least 2 controller replicas remain available during upgrades.
