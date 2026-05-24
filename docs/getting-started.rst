Quickstart
==========

The fastest way to try Spur. Everything runs on one machine, no config file needed.

Get the Binaries
----------------

.. code-block:: bash

   curl -fsSL https://raw.githubusercontent.com/ROCm/spur/main/install.sh | bash
   export PATH="$HOME/.local/bin:$PATH"

Or build from source — see :doc:`/developer/building`.

Start the Controller
--------------------

.. code-block:: bash

   mkdir -p /tmp/spur-state
   spurctld -D --state-dir /tmp/spur-state --log-level info

The controller listens on port 6817 and creates a single "default" partition.

Start the Agent
---------------

In a new terminal:

.. code-block:: bash

   spurd -D --controller http://localhost:6817

The agent auto-discovers local CPUs, memory, and GPUs, then registers with the controller:

.. code-block:: text

   INFO spurd: resources discovered cpus=16 memory_mb=32000 gpus=0
   INFO spurd::reporter: registered with controller

Submit a Job
------------

.. code-block:: bash

   cat > /tmp/hello.sh << 'EOF'
   #!/bin/bash
   #SBATCH --job-name=hello
   #SBATCH --time=00:01:00

   echo "Hello from Spur!"
   echo "Running on $(hostname) with $SPUR_CPUS_ON_NODE CPUs"
   sleep 2
   echo "Done."
   EOF

   spur submit /tmp/hello.sh

Output: ``Submitted batch job 1``

Check Status
------------

.. code-block:: bash

   spur queue              # View the job queue
   spur nodes              # View nodes
   spur show job 1         # Detailed job info
   cat /tmp/spur-1.out     # Job output (default location)

Run an Interactive Command
--------------------------

.. code-block:: bash

   spur run hostname
   spur run -- bash -c "echo I have \$SPUR_CPUS_ON_NODE CPUs"

Slurm Compatibility
-------------------

Use Slurm commands as ``spur`` subcommands:

.. code-block:: bash

   spur sbatch /tmp/hello.sh
   spur squeue
   spur sinfo -N
   spur scancel 2

Or create symlinks for full drop-in compatibility:

.. code-block:: bash

   cd $(dirname $(which spur))
   for cmd in sbatch srun squeue scancel sinfo sacct scontrol; do
       ln -sf spur $cmd
   done
