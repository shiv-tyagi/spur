Contributing
============

Getting Started
---------------

1. Fork the repository on GitHub
2. Clone your fork and add the upstream remote:

.. code-block:: bash

   git clone https://github.com/<your-username>/spur.git
   cd spur
   git remote add upstream https://github.com/ROCm/spur.git

3. Follow the `building guide <building.rst>`_ to build the project and run tests. Get familiar with the build and test workflow before making changes.

4. Before starting work, sync with upstream:

.. code-block:: bash

   git fetch upstream
   git rebase upstream/main

Commit Style
------------

Use conventional commits: ``<type>(<scope>): <description>``

**type** — one of: ``feat``, ``fix``, ``refactor``, ``test``, ``docs``, ``style``, ``perf``, ``build``, ``ci``, ``chore``, ``revert``

**scope** — the crate name (e.g. ``spur-cli``, ``spurctld``, ``spur-core``, ``spur-k8s``). If no single crate applies, use a concise scope reflecting the area of change (``proto``, ``deploy``, ``config``).

**description** — imperative mood, lowercase, no trailing period. A short description of what your changes do.

Example:

.. code-block:: text

   feat(spur-k8s): add support for GPU resource limits in SpurJob spec

Add a body paragraph only when the "why" isn't obvious from the subject line.

License Headers
---------------

All new source files (``.rs``, ``.proto``, ``.py``, ``.sh``) must include this header at the top:

.. code-block:: text

   // Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
   // SPDX-License-Identifier: Apache-2.0

Use ``#`` instead of ``//`` for Python and shell scripts.

Pre-Commit Hooks
----------------

The repo includes pre-commit and commit-msg hooks that check formatting, license headers, and commit message format. Opt in by pointing git to the hooks directory:

.. code-block:: bash

   git config core.hooksPath .githooks

This enforces:

- ``cargo fmt --check --all`` — code formatting
- SPDX license headers on staged source files
- Commit message matches conventional commit format (see above)

If formatting fails, run ``cargo fmt --all`` and amend your commit. You can bypass hooks with ``git commit --no-verify``, but CI will still enforce these checks.

PR Process
----------

1. Create a feature branch from ``main`` on your fork
2. Make your changes in logical commits following the commit style above
3. Ensure all pre-commit hook checks pass
4. Push to your fork and open a PR against ``ROCm/spur:main``

.. note::

   All PRs must pass CI checks to be eligible for merging. If you see a CI test failing that doesn't look related to your changes, tag a maintainer in the PR for help.

Review Process
--------------

Maintainers look for clear code structure, adequate test coverage, and long-term maintainability. In particular:

- New functionality has corresponding tests
- Features are complete end-to-end within a single PR. Avoid partial implementations that leave unused code or require follow-up PRs to become functional
- Commits are logically separated and follow the commit style
- The PR description explains what changed and why
