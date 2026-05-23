Installation
============

One-Line Install
----------------

Download the latest release binaries:

.. code-block:: bash

   curl -fsSL https://raw.githubusercontent.com/ROCm/spur/main/install.sh | bash

This installs to ``~/.spur/bin``. Add it to your PATH:

.. code-block:: bash

   export PATH="$HOME/.spur/bin:$PATH"

Build from Source
-----------------

Prerequisites:

- Rust 1.75+ (install via `rustup <https://rustup.rs>`_)
- Protocol Buffers compiler

.. code-block:: bash

   # Install protobuf compiler
   sudo apt install protobuf-compiler    # Debian/Ubuntu
   sudo dnf install protobuf-compiler    # Fedora/RHEL

   # Build
   cargo build --release

Binaries land in ``target/release/``:

.. list-table::
   :header-rows: 1

   * - Binary
     - Role
   * - ``spur``
     - CLI (multi-call binary: also acts as sbatch, squeue, etc.)
   * - ``spurctld``
     - Controller daemon — scheduling, state, Raft consensus
   * - ``spurd``
     - Node agent — runs on every compute node
   * - ``spurdbd``
     - Accounting daemon (optional, needs PostgreSQL)
   * - ``spurrestd``
     - REST API daemon (optional)
   * - ``libspur_compat.so``
     - C FFI shim — drop-in for libslurm.so

Portable Binaries
-----------------

Build glibc 2.28+ compatible binaries (works on Ubuntu 20.04+, RHEL 8+, Debian 10+):

.. code-block:: bash

   ./deploy/build-portable.sh    # outputs to dist/bin/

This uses Docker to build against an older glibc, producing binaries that run on a wider range of distros.

Docker
------

.. code-block:: bash

   # Build the image
   docker build -t spur .

   # Run CLI commands
   docker run --rm spur sinfo

   # Run the controller
   docker run -d --name spurctld -p 6817:6817 spur spurctld --listen=[::]:6817

For Kubernetes deployment, see ``deploy/k8s/``.
